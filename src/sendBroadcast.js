const AWS = require('aws-sdk');
const { default: PQueue } = require('p-queue');
const { DateTime } = require('luxon');
const {
	fullScanForDynamoDB,
	ReturnType: ScanReturnType,
} = require('full-scan-for-dynamodb');
const { queryAllForDynamoDB, ReturnType } = require('query-all-for-dynamodb');
const dbTablePrefix = process.env.DB_TABLE_PREFIX || '';
const { writeAllForDynamoDB } = require('write-all-for-dynamodb');

const getSubscribers = require('./getSubscribers');
const createAutoMergedTemplate = require('./createAutoMergedTemplate');

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

const winningTypes = {
	AutoMergeSubjectAndContent: 'auto-merge subject (most opens) and content (most clicks) into new email',
	Content: 'content: email with most clicks',
	Subject: 'subject: email with most opens',
}

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

const maxEmailsPerSecondRaw = process.env.MAX_EMAILS_PER_SECOND || '40';
const maxEmailsPerSecond = !isNaN(parseFloat(maxEmailsPerSecondRaw)) ?
	parseFloat(maxEmailsPerSecondRaw) :
	40;

if (maxEmailsPerSecond <= 0) {
	throw new Error(`MAX_EMAILS_PER_SECOND must be a positive integer`)
}

const delayBetweenEmails = 1000.0 / maxEmailsPerSecond;

console.info(`Configured to schedule broadcast emails at a max of rate of
${maxEmailsPerSecond} emails per second. That is, an average delay of ${
	delayBetweenEmails.toFixed(2)} milliseconds between broadcast emails`);

function getSubscriberTimezone(subscriber) {
	return subscriber.timezone || 'America/Los_Angeles';
}

/**
 * 
 * @param {*} timezone 
 * @param {DateTime} referenceTimestampDatetime 
 * @param {*} delay 
 * @returns 
 */
function getRunAtForTimezone(timezone, subscriberRunAt, delay) {
	try {
		const datetimeInTimezone = DateTime.fromISO(subscriberRunAt, {
			zone: timezone
		});
		if (!datetimeInTimezone.isValid) {
			// Subscriber probably doesn't have a valid timezone, use generic time
			return parseInt(Date.parse(subscriberRunAt) + delay)
		}
		return datetimeInTimezone.plus({ milliseconds: delay }).valueOf();
	} catch (err) {
		console.error(err.message);
		// Something went wrong using the timezone, so just parse with system
		// timezone:
		return parseInt(Date.parse(subscriberRunAt) + delay)
	}
}

const queueItemsForSubscriberGroup = async (subscriberGroup, broadcastData) => {
	const { useSubscriberTime, subscriberRunAt } = broadcastData;
	await writeAllForDynamoDB(db, {
		RequestItems: {
			[`${dbTablePrefix}Queue`]: subscriberGroup.map((subscriber, i) => {
				const delay = i * delayBetweenEmails;
				// Use delay between emails to ensure broadcast emails remain under
				// max emails per minute.
				const runAt = useSubscriberTime ?
					getRunAtForTimezone(getSubscriberTimezone(subscriber), subscriberRunAt, delay) :
					parseInt(broadcastData.validRunAt + delay)
				const Item = newQueueItem(
					{
						type: 'send email',
						subscriber: subscriber,
						subscriberId: subscriber.subscriberId,
						templateId: broadcastData.templateId,
						tagReason: broadcastData.tags,
						startDate: broadcastData.startDate,
						broadcastRunAtId: broadcastData.broadcastRunAtId,
						tagOnClick: broadcastData.tagOnClick,
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

const updateSubscriberPendingBroadcasts = async (
	subscriberId,
	currentPendingBroadcasts,
	newPendingBroadcast
) => {
	const newPendingBroadcasts = (Array.isArray(currentPendingBroadcasts) &&
		currentPendingBroadcasts.concat(newPendingBroadcast)) || [
		newPendingBroadcast,
	];
	await db
		.update({
			TableName: `${dbTablePrefix}Subscribers`,
			Key: { subscriberId },
			UpdateExpression: 'set #pendingBroadcasts = :pendingBroadcasts',
			ExpressionAttributeNames: { '#pendingBroadcasts': 'pendingBroadcasts' },
			ExpressionAttributeValues: {
				':pendingBroadcasts': newPendingBroadcasts,
			},
		})
		.promise();
};

const updateSubscriberRemoveBroadcast = async (
	subscriberId,
	newPendingBroadcasts,
) => {
	await db
		.update({
			TableName: `${dbTablePrefix}Subscribers`,
			Key: { subscriberId },
			UpdateExpression: 'set #pendingBroadcasts = :pendingBroadcasts',
			ExpressionAttributeNames: { '#pendingBroadcasts': 'pendingBroadcasts' },
			ExpressionAttributeValues: { ':pendingBroadcasts': newPendingBroadcasts },
		})
		.promise();
}

const sendSingleTemplateBroadcast = async (broadcastData) => {
	const subscribers = await getSubscribers({
		tags: broadcastData.tags,
		excludeTags: broadcastData.excludeTags,
		properties: broadcastData.properties,
		interactions: broadcastData.interactions,
		pendingBroadcast: broadcastData.broadcastRunAtId,
		interactionWithAnyEmail: broadcastData.interactionWithAnyEmail,
		ignoreConfirmed: broadcastData.ignoreConfirmed,
		joinedAfter: broadcastData.joinedAfter,
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
			console.log(
				`group ${groupNumber++}: ${group.subscribers.length} subscribers`
			);
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

const updateBroadcastPhase = async (broadcastData, patch) => {
	console.log('deleting');
	console.log(patch);
	console.log(broadcastData);
	await db
		.delete({
			TableName: `${dbTablePrefix}BroadcastQueue`,
			Key: { phase: broadcastData.phase, runAt: broadcastData.runAt },
		})
		.promise();
	const Item = Object.assign({}, broadcastData, patch);
	console.log('updating');
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
	let clicksWinner = null;
	let opensWinner = null;
	let clicksToOpensWinner = null;
	const splitTestResults = [];
	results.forEach((templateResult, templateName) => {
		const templateClickRatio = templateResult.sends > 0 ?
			(templateResult.clicks / templateResult.sends) :
			0;
		const templateOpenRatio = templateResult.sends > 0 ?
			(templateResult.opens / templateResult.sends) :
			0;
		const templateClickToOpenRatio = templateResult.opens > 0 ?
			(templateResult.clicks / templateResult.opens) :
			0;
		splitTestResults.push(
			Object.assign({}, templateResult, {
				templateName,
				templateClickRatio,
				templateOpenRatio,
				templateClickToOpenRatio,
			})
		);
		if (!clicksWinner || clicksWinner.clickRatio < templateClickRatio) {
			clicksWinner = {
				clickRatio: templateClickRatio,
				templateName,
			};
		}
		if (!opensWinner || opensWinner.openRatio < templateOpenRatio) {
			opensWinner = {
				openRatio: templateOpenRatio,
				templateName,
			}
		}
		if (!clicksToOpensWinner || clicksToOpensWinner.clicksToOpensRatio) {
			clicksToOpensWinner = {
				clicksToOpensRatio: templateClickToOpenRatio,
				templateName,
			}
		}
	});
	let winningTemplateName = '';
	if (
		opensWinner &&
		broadcastData.winningType === winningTypes.Subject
	) {
		winningTemplateName = opensWinner.templateName;
	} else if(
		opensWinner &&
		opensWinner.templateName !== clicksToOpensWinner.templateName &&
		broadcastData.winningType === winningTypes.AutoMergeSubjectAndContent
	) {
		const subjectTemplateName = opensWinner.templateName
		const contentTemplateName = clicksToOpensWinner.templateName
		try {
			winningTemplateName = await createAutoMergedTemplate({
				subjectTemplateName,
				contentTemplateName,
			})
		} catch (err) {
			console.error(err);
			// If there was an error creating a merged template, just use the clicks
			// winner
			console.warn(
				'Error while merging winning templates; falling back to clicks winner.'
			)
			winningTemplateName = contentTemplateName;
		}
	} else {
		winningTemplateName = clicksToOpensWinner.templateName;
	}
	return {
		winningTemplate: winningTemplateName,
		splitTestResults,
	};
};

const removePendingBroadcast = async (pendingBroadcast) => {
	const queue = new PQueue({ concurrency: 16 });
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
				const { subscriberId, pendingBroadcasts } = subscriber
				const subscriberRemoveBroadcast = async () => updateSubscriberRemoveBroadcast(
					subscriberId,
					pendingBroadcasts
				)
				queue.add(subscriberRemoveBroadcast)
			},
		}
	);
	await queue.onIdle();
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
			joinedAfter: broadcastData.joinedAfter,
		});
		if (!subscribers || !subscribers.length) {
			await updateBroadcastPhase(broadcastData, {
				phase: 'skipped',
				skipReason: 'No subscribers were found',
			});
			return;
		}
		console.log(`Sending to ${subscribers.length} subscribers`);
		const testDurationHours = (
			process.env.TEST_DURATION_HOURS &&
			!isNaN(parseFloat(process.env.TEST_DURATION_HOURS)) &&
			parseFloat(process.env.TEST_DURATION_HOURS)
		) || 4;
		const newRunAt = `${testDurationHours * 60 * 60 * 1000 + broadcastData.validRunAt}.${
			broadcastData.runAt.split('.')[1]
		}`;
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'starting-test',
			runAt: newRunAt,
		});
		const initialSendGroup = [];
		let subscriber = subscribers.pop();
		const queue = new PQueue({ concurrency: 16 });
		const testListPercentage = (
			process.env.TEST_LIST_PERCENTAGE &&
			!isNaN(parseFloat(process.env.TEST_LIST_PERCENTAGE)) &&
			parseFloat(process.env.TEST_LIST_PERCENTAGE)
		) || 33;
		while (subscriber) {
			if (Math.floor(Math.random() * 100) < testListPercentage) {
				initialSendGroup.push(subscriber);
			} else {
				const { subscriberId, pendingBroadcasts } = subscriber
				const subscriberUpdate = async () => updateSubscriberPendingBroadcasts(
					subscriberId,
					pendingBroadcasts,
					newRunAt
				)
				queue.add(subscriberUpdate);
			}
			subscriber = subscribers.pop();
		}
		await queue.onIdle();
		await sendVariableTemplatesBroadcastToSubscribers(
			broadcastData,
			initialSendGroup
		);
		await updateBroadcastPhase(broadcastData, {
			phase: 'in-test',
			runAt: newRunAt,
		});
		return;
	} else if (broadcastData.phase === 'in-test') {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'determining-winning-template',
		});
		const { winningTemplate, splitTestResults } = await getWinningTemplate(
			broadcastData
		);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sending',
			templateId: winningTemplate,
			splitTestResults,
		});
		delete broadcastData.templates;
		await sendSingleTemplateBroadcast(broadcastData);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'cleaning-up',
		});
		await removePendingBroadcast(broadcastData.broadcastRunAtId);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sent',
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
	if (broadcastData.datetimeContext === 'subscriber') {
		if (
			typeof broadcastData.subscriberRunAt !== 'string' ||
			!(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(broadcastData.subscriberRunAt)) ||
			!(DateTime.fromISO(broadcastData.subscriberRunAt).isValid)
		) {
			await updateBroadcastPhase(broadcastData, {
				phase: 'error',
				errorReason: 'Invalid broadcast subscriberRunAt value - not a valid date format',
			});
			return;
		}
		const broadcastRunAtTimestamp = parseInt(broadcastData.runAt);
		if (isNaN(broadcastRunAtTimestamp)) {
			await updateBroadcastPhase(broadcastData, {
				phase: 'error',
				errorReason: 'Invalid broadcast runAt value - NaN',
			});
			return;
		}
		broadcastData.validRunAt = broadcastRunAtTimestamp;
		broadcastData.startDate = broadcastData.subscriberRunAt.substring(0, 10);
		broadcastData.useSubscriberTime = true;
	} else {
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
		broadcastData.useSubscriberTime = false;
	}

	if (typeof broadcastData.templateId === 'string') {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sending-broadcast',
		});
		await sendSingleTemplateBroadcast(broadcastData);
		broadcastData = await updateBroadcastPhase(broadcastData, {
			phase: 'sent',
		});
	} else if (Array.isArray(broadcastData.templates)) {
		broadcastData = await updateBroadcastPhase(broadcastData, {
			broadcastRunAtId: broadcastData.broadcastRunAtId || broadcastData.runAt,
		});
		await sendVariableTemplatesBroadcast(broadcastData);
	} else {
		throw new Error(`Invalid broadcast data: ${JSON.stringify(broadcastData)}`);
	}
};

module.exports = sendBroadcast;
