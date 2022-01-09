const AWS = require('aws-sdk');
const { fullScanForDynamoDB } = require('full-scan-for-dynamodb');
const { queryAllForDynamoDB, ReturnType } = require('query-all-for-dynamodb');
const dbTablePrefix = process.env.DB_TABLE_PREFIX || '';

// Create DB connection
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
 *
 * @param {Set[]} sets
 */
function intersection(sets) {
	const intersectionSet = new Set();
	if (!sets[0]) {
		return;
	}
	for (let item of sets[0]) {
		intersectionSet.add(item);
	}
	if (!sets[1]) {
		return intersectionSet;
	}
	[...intersectionSet].forEach((item) => {
		for (let i = 1; i < sets.length; i++) {
			const set = sets[i];
			if (!set.has(item)) {
				intersectionSet.delete(item);
				continue;
			}
		}
	});
	return intersectionSet;
}

const getFilteredSubscribersFromDb = async (opts) => {
	const scanParams = {
		TableName: `${dbTablePrefix}Subscribers`,
		FilterExpression:
			'(#unsub = :false2 or attribute_not_exists(#unsub)) ' +
			'and (not (contains(#tags, :unsubscribed)))',
		ExpressionAttributeNames: {
			'#unsub': 'unsubscribed',
			'#tags': 'tags',
		},
		ExpressionAttributeValues: {
			':false2': false,
			':unsubscribed': 'unsubscribed',
		},
	};
	if (!opts.ignoreConfirmed) {
		scanParams.FilterExpression += ' and (#confirmed <> :false1)';
		scanParams.ExpressionAttributeNames['#confirmed'] = 'confirmed';
		scanParams.ExpressionAttributeValues[':false1'] = false;
	}
	if (opts.joinedAfter) {
		scanParams.FilterExpression += ' and #joined >= :joinedAfter';
		scanParams.ExpressionAttributeNames['#joined'] = 'joined';
		scanParams.ExpressionAttributeValues[':joinedAfter'] = opts.joinedAfter;
	}
	if (
		opts.interactionWithAnyEmail &&
		typeof opts.interactionWithAnyEmail === 'object' &&
		opts.interactionWithAnyEmail.interactionPeriodUnit === 'days' &&
		typeof opts.interactionWithAnyEmail.interactionPeriodValue === 'number'
	) {
		const duration = opts.interactionWithAnyEmail.interactionPeriodValue * 24 * 60 * 60 * 1000
		const thresholdTimestamp = Date.now() - duration
		if (opts.interactionWithAnyEmail.interactionType === 'opened or clicked') {
			scanParams.FilterExpression +=
				' and (#lastOpenOrClick >= :openOrClickThreshold)';
			scanParams.ExpressionAttributeNames['#lastOpenOrClick'] =
				'lastOpenOrClick';
			scanParams.ExpressionAttributeValues[':openOrClickThreshold'] =
				thresholdTimestamp;
		} else if (opts.interactionWithAnyEmail.interactionType === 'clicked') {
			scanParams.FilterExpression += ' and (#lastClick >= :lastClickThreshold)';
			scanParams.ExpressionAttributeNames['#lastClick'] = 'lastClick';
			scanParams.ExpressionAttributeValues[':lastClickThreshold'] =
				thresholdTimestamp;
		}
	}
	if (opts.pendingBroadcast) {
		scanParams.FilterExpression += 
			' and contains(#pendingBroadcasts, :pendingBroadcast)';
		scanParams.ExpressionAttributeNames['#pendingBroadcasts'] = 'pendingBroadcasts';
		scanParams.ExpressionAttributeValues[':pendingBroadcast'] = opts.pendingBroadcast;
	}
	if (Object.prototype.hasOwnProperty.call(opts, 'consistentRead')) {
		scanParams.ConsistentRead = opts.consistentRead;
	}
	if (Array.isArray(opts.tags)) {
		scanParams['FilterExpression'] += opts.tags
			.map((tag, index) => ` and contains(#tags, :tag${index})`)
			.join('');
		opts.tags.forEach((tag, index) => {
			scanParams['ExpressionAttributeValues'][`:tag${index}`] = tag;
		});
	}
	if (Array.isArray(opts.excludeTags)) {
		scanParams['FilterExpression'] += opts.excludeTags
			.map((tag, index) => ` and not(contains(#tags, :excludeTag${index}))`)
			.join('');
		opts.excludeTags.forEach((tag, index) => {
			scanParams['ExpressionAttributeValues'][`:excludeTag${index}`] = tag;
		});
	}
	if (typeof opts.properties === 'object') {
		let index = 0;
		for (let prop in opts.properties) {
			scanParams['FilterExpression'] += ` and #prop${index} = :prop${index}`;
			scanParams['ExpressionAttributeNames'][`#prop${index}`] = prop;
			scanParams['ExpressionAttributeValues'][`:prop${index}`] =
				opts.properties[prop];
			index++;
		}
	}
	const subscribers = await fullScanForDynamoDB(db, scanParams);
	return subscribers;
};

const getSubscribersIdsMatchingInteractionFilter = async (interaction) => {
	console.log('match interaction')
	console.log(interaction)
	const subscribeIdSet = new Set();
	const queryParams = {
		TableName: `${dbTablePrefix}Queue`,
		KeyConditionExpression: '#queuePlacement = :sendDate',
		FilterExpression: '#templateId = :templateId',
		ExpressionAttributeNames: {
			'#queuePlacement': 'queuePlacement',
			'#templateId': 'templateId',
		},
		ExpressionAttributeValues: {
			':sendDate': interaction.emailDate,
			':templateId': interaction.templateId,
		},
		ProjectionExpression: 'subscriberId',
	};
	if (typeof interaction.click !== 'undefined') {
		queryParams.ExpressionAttributeNames['#click'] = 'click';
		if (interaction.click) {
			queryParams.FilterExpression += ' and attribute_exists(#click)';
		} else {
			queryParams.FilterExpression += ' and attribute_not_exists(#click)';
		}
	}
	if (typeof interaction.open !== 'undefined') {
		queryParams.ExpressionAttributeNames['#open'] = 'open';
		if (interaction.open) {
			queryParams.FilterExpression += ' and attribute_exists(#open)';
		} else {
			queryParams.FilterExpression += ' and attribute_not_exists(#open)';
		}
	}
	await queryAllForDynamoDB(db, queryParams, {
		onEachItem: (item) => {
			subscribeIdSet.add(item.subscriberId);
		},
		returnType: ReturnType.none,
	});
	return subscribeIdSet;
};

const getSubscribersIdsMatchingInteractionFilters = async (interactions) => {
	const subscriberIdSets = await Promise.all(
		interactions.map((interaction) =>
			getSubscribersIdsMatchingInteractionFilter(interaction)
		)
	);
	if (subscriberIdSets.length === 1) {
		return subscriberIdSets[0];
	}
	const intersectionSet = intersection(subscriberIdSets);
	return intersectionSet;
};

const getSubscribers = async (opts) => {
	console.log('Searching for subscribers with properties');
	console.log(opts);
	const [subscriberIds, subscriberPool] = await Promise.all([
		Promise.resolve(
			opts.interactions &&
				opts.interactions.length &&
				getSubscribersIdsMatchingInteractionFilters(opts.interactions)
		),
		getFilteredSubscribersFromDb(opts),
	]);
	if (!opts.interactions || !opts.interactions.length) {
		console.log(`${subscriberPool.length} subscribers found`);
		return subscriberPool;
	}
	const finalSubscribers = [];
	let subscriber = subscriberPool.pop();
	if (!subscriberIds || !subscriberIds.size) {
		return finalSubscribers;
	}
	while (subscriber) {
		if (subscriberIds.has(subscriber.subscriberId)) {
			finalSubscribers.push(subscriber);
		}
		subscriber = subscriberPool.pop();
	}
	console.log(`${finalSubscribers.length} subscribers found`);
	return finalSubscribers;
};

module.exports = getSubscribers;
