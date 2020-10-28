const AWS = require('aws-sdk');
const {
	fullScanForDynamoDB,
	ReturnType: ScanReturnType,
} = require('full-scan-for-dynamodb');
const { queryAllForDynamoDB, ReturnType } = require('query-all-for-dynamodb');
const dbTablePrefix = process.env.DB_TABLE_PREFIX || '';
const { writeAllForDynamoDB } = require('write-all-for-dynamodb');

const debugLog = require('./debugLog');
const getSubscribers = require('./getSubscribers');

const dbParams = {
	region: process.env.AWS_REGION,
};
if (process.env.DYNAMODB_ENDPOINT) {
	dbParams.endpoint = process.env.DYNAMODB_ENDPOINT;
} else {
	dbParams.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
	dbParams.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
}

const db = new AWS.DynamoDB.DocumentClient(dbParams);

/**
 * Creates a new queue item.
 */
const newQueueItem = (itemData, runAt) => {
	const runAtModified = `${runAt}${Math.random().toString().substring(1)}`;
	return Object.assign({}, itemData, {
		queuePlacement: 'queued',
		runAtModified,
		runAt,
		attempts: 0,
		failed: false,
		completed: false,
	});
};

const queueItemsForSubscriberGroup = async (subscriberGroup, broadcastData) => {
	const startTime = Date.now();
	await writeAllForDynamoDB(db, {
		RequestItems: {
			[`${dbTablePrefix}Queue`]: subscriberGroup.map((subscriber, i) => {
				// Run at increases with the time offset and the square root of the number of subscribers
				// to avoid id collision when generating queue items.
				const runAt = parseInt(
					broadcastData.validRunAt + (Date.now() - startTime) + Math.sqrt(i)
				);
				const Item = newQueueItem(
					{
						type: 'send email',
						subscriber: subscriber,
						subscriberId: subscriber.subscriberId,
						templateId: broadcastData.templateId,
						tagReason: broadcastData.tags,
						startDate: broadcastData.startDate,
						broadcastRunAtId: broadcastData.broadcastRunAtId,
					},
					runAt
				);
				if (broadcastData.nextRunAt) {
					Item.nextRunAt = broadcastData.nextRunAt;
				}
				return {
					PutRequest: {
						Item,
					},
				};
			}),
		},
	});
};

const sendSingleTemplateBroadcast = async (broadcastData) => {
	const subscribers = await getSubscribers({
		tags: broadcastData.tags,
		excludeTags: broadcastData.excludeTags,
		properties: broadcastData.properties,
		interactions: broadcastData.interactions,
		pendingBroadcast: broadcastData.broadcastRunAtId,
		interactionWithAnyEmail: broadcastData.interactionWithAnyEmail,
		ignoreConfirmed: broadcastData.ignoreConfirmed,
	});
	if (!subscribers || !subscribers.length) {
		return 0;
	}
	await queueItemsForSubscriberGroup(subscribers, broadcastData);
};

const sendVariableTemplatesBroadcastToSubscribers = async (
	broadcastData,
	subscribers
) => {
	const templates = broadcastData.templates;
	const templatesLength = templates.length;
	const subscriberTemplateGroups = [];
	for (let i = 0; i < templatesLength; i++) {
		subscriberTemplateGroups.push({
			templateId: templates[i].name,
			subscribers: [],
		});
	}
	let subscriber = subscribers.pop();
	while (subscriber) {
		const templateIndex = Math.floor(Math.random() * templatesLength);
		subscriberTemplateGroups[templateIndex].subscribers.push(subscriber);
		subscriber = subscribers.pop();
	}
	let group = subscriberTemplateGroups.pop();
	let sendData = [];
	// Could be parallel, but just send 1 group at a time to avoid hitting AWS limits
	// for writing to the database.
	let groupNumber = 1;
	while (group) {
		if (group.subscribers.length) {
			console.log(`group ${groupNumber++}: ${group.subscribers.length} subscribers`)
			const groupBroadcastData = Object.assign({}, broadcastData, {
				templateId: group.templateId,
			});
			delete groupBroadcastData.templates;
			await queueItemsForSubscriberGroup(group.subscribers, groupBroadcastData);
			sendData.push({
				templateId: group.templateId,
				subscriberCount: group.subscribers.length,
			});
		}
		group = subscriberTemplateGroups.pop();
	}
	return sendData;
};

const updateBroadcastPhase = async (
	broadcastData,
	patch
) => {
	console.log('deleting')
	console.log(patch)
	console.log(broadcastData)
	await db
		.delete({
			TableName: `${dbTablePrefix}BroadcastQueue`,
			Key: { phase: broadcastData.phase, runAt: broadcastData.runAt },
		})
		.promise();
	const Item = Object.assign({}, broadcastData, patch);
	console.log('updating')
	await db
		.put({
			TableName: `${dbTablePrefix}BroadcastQueue`,
			Item,
		})
		.promise();
	return Item;
};

const getWinningTemplate = async (broadcastData) => {
	const results = new Map();
	broadcastData.templates.forEach((template) => {
		results.set(template.name, {
			opens: 0,
			clicks: 0,
			opensOrClicks: 0,
			sends: 0,
		});
	});
	await queryAllForDynamoDB(
		db,
		{
			TableName: `${dbTablePrefix}Queue`,
			KeyConditionExpression: '#queuePlacement = :startDate',

			ExpressionAttributeNames: {
				'#queuePlacement': 'queuePlacement',
			},
			ExpressionAttributeValues: {
				':startDate': broadcastData.startDate,
			},
		},
		{
			returnType: ReturnType.none,
			onEachItem: (item) => {
				const templateResults = results.get(item.templateId);
				if (templateResults) {
					templateResults.sends += 1;
					if (item.open) {
						templateResults.opens += 1;
					}
					if (item.click) {
						templateResults.clicks += 1;
					}
					if (item.open || item.click) {
						templateResults.opensOrClicks += 1;
					}
				}
			},
		}
	);
	let winner = null;
	const splitTestResults = [];
	results.forEach((templateResult, templateName) => {
		const templateClickRatio =
			templateResult.clicks / (templateResult.sends || 1);
		splitTestResults.push(
			Object.assign({}, templateResult, { templateName, templateClickRatio })
		);
		if (!winner || winner.clickRatio < templateClickRatio) {
			winner = {
				clickRatio: templateClickRatio,
				templateName,
			};
		}
	});
	return {
		winningTemplate: winner.templateName,
		splitTestResults,
	};
};

const removePendingBroadcast = async (pendingBroadcast) => {
	await fullScanForDynamoDB(
		db,
		{
			TableName: `${dbTablePrefix}Subscribers`,
			FilterExpression: `contains(#pendingBroadcasts, :pendingBroadcast)`,
			ExpressionAttributeNames: {
				'#pendingBroadcasts': 'pendingBroadcasts',
			},
			ExpressionAttributeValues: {
				':pendingBroadcast': pendingBroadcast,
			},
		},
		{
			returnType: ScanReturnType.none,
			onEachItem: async (subscriber) => {
				if (!Array.isArray(subscriber.pendingBroadcasts)) {
					return;
				}
				const index = subscriber.pendingBroadcasts.indexOf(pendingBroadcast);
				if (index < 0) {
					return;
				}
				subscriber.pendingBroadcasts.splice(index, 1);
				await db
					.update({
						TableName: `${dbTablePrefix}Subscribers`,
						Key: {
							subscriberId: subscriber.subscriberId,
						},
						UpdateExpression: 'set #pendingBroadcasts = :pendingBroadcasts',
						ExpressionAttributeNames: {
							'#pendingBroadcasts': 'pendingBroadcasts',
						},
						ExpressionAttributeValues: {
							':pendingBroadcasts': subscriber.pendingBroadcasts,
						},
					})
					.promise();
			},
		}
	);
};

const sendVariableTemplatesBroadcast = async (broadcastData) => {
	if (broadcastData.phase === 'pending') {
		let totalPercentages = 0;
		for (let i = 0; i < broadcastData.templates.length; i++) {
			const template = broadcastData.templates[i];
			if (typeof template.name !== 'string' || !template.name) {
				await updateBroadcastPhase(broadcastData, {
					phase: 'skipped',
					skipReason: 'A template had no name',
				});
				return;
			}
			if (
				typeof template.testPercent !== 'number' ||
				isNaN(template.testPercent) ||
				template.testPercent < 0 ||
				template.testPercent > 100
			) {
				await updateBroadcastPhase(broadcastData, {
					phase: 'skipped',
					skipReason: 'Invalid test percentage assigned to a template',
				});
				return;
			}
			totalPercentages += template.testPercent;
		}
		if (Math.round(totalPercentages) !== 100) {
			await updateBroadcastPhase(broadcastData, {
				phase: 'skipped',
				skipReason: 'test percetages for templates do not sum to 100',
			});
			return;
		}
		const subscribers = await getSubscribers({
			tags: broadcastData.tags,
			excludeTags: broadcastData.excludeTags,
			properties: broadcastData.properties,
			interactions: broadcastData.interactions,
			interactionWithAnyEmail: broadcastData.interactionWithAnyEmail,
			ignoreConfirmed: broadcastData.ignoreConfirmed,
		});
		if (!subscribers || !subscribers.length) {
			await updateBroadcastPhase(broadcastData, {
				phase: 'skipped',
				skipReason: 'No subscribers were found',
			});
			return;
		}
		console.log(`Sending to ${subscribers.length} subscribers`)
		const newRunAt = `${4 * 60 * 60 * 1000 + broadcastData.validRunAt}.${
			broadcastData.runAt.split('.')[1]
		}`;
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'starting-test',
			runAt: newRunAt,
		});
		const initialSendGroup = [];
		const laterSendGroupSaves = [];
		let subscriber = subscribers.pop();
		while (subscriber) {
			if (Math.floor(Math.random() * 10) % 2 === 0) {
				laterSendGroupSaves.push(
					db
						.update({
							TableName: `${dbTablePrefix}Subscribers`,
							Key: {
								subscriberId: subscriber.subscriberId,
							},
							UpdateExpression: 'set #pendingBroadcasts = :pendingBroadcasts',
							ExpressionAttributeNames: {
								'#pendingBroadcasts': 'pendingBroadcasts',
							},
							ExpressionAttributeValues: {
								':pendingBroadcasts': (Array.isArray(
									subscriber.pendingBroadcasts
								) &&
									subscriber.pendingBroadcasts.concat(newRunAt)) || [newRunAt],
							},
						})
						.promise()
				);
			} else {
				initialSendGroup.push(subscriber);
			}
			subscriber = subscribers.pop();
		}
		const [sendLaterResults, sendData] = await Promise.all([
			Promise.all(laterSendGroupSaves),
			sendVariableTemplatesBroadcastToSubscribers(
				broadcastData,
				initialSendGroup
			),
		]);
		await updateBroadcastPhase(broadcastData, {
			phase: 'in-test',
			runAt: newRunAt,
			sendData,
		});
		return;
	} else if (broadcastData.phase === 'in-test') {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'determining-winning-template'
		});
		const { winningTemplate, splitTestResults } = await getWinningTemplate(broadcastData);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sending',
			templateId: winningTemplate,
			splitTestResults,
		});
		delete broadcastData.templates;
		await sendSingleTemplateBroadcast(broadcastData);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'cleaning-up'
		});
		await removePendingBroadcast(broadcastData.broadcastRunAtId);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sent'
		});
		return;
	}
};

/**
 * Sends a broadcast email to all applicable subscribers.
 * @param  {Object} broadcastData Defines the broadcast to send.
 * @return {Promise}
 */
const sendBroadcast = async (broadcastData) => {
	const broadcastRunAtTimestamp = parseInt(broadcastData.runAt);
	if (isNaN(broadcastRunAtTimestamp)) {
		await updateBroadcastPhase(broadcastData, {
			phase: 'error',
			errorReason: 'Invalid broadcast runAt value - NaN',
		});
		return;
	}
	broadcastData.validRunAt = broadcastRunAtTimestamp;
	broadcastData.startDate = new Date(broadcastData.validRunAt)
		.toISOString()
		.substring(0, 10);
	if (typeof broadcastData.templateId === 'string') {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sending-broadcast',
		});
		await sendSingleTemplateBroadcast(broadcastData);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sent'
		});
	} else if (Array.isArray(broadcastData.templates)) {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			broadcastRunAtId: broadcastData.broadcastRunAtId || broadcastData.runAt,
		})
		await sendVariableTemplatesBroadcast(broadcastData);
	} else {
		throw new Error(`Invalid broadcast data: ${JSON.stringify(broadcastData)}`);
	}
};

module.exports = sendBroadcast;
