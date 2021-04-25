require('dotenv').config();
const AWS = require('aws-sdk');
AWS.config = {
	region: process.env.AWS_REGION,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
	}
}
const sendBroadcast = require('./sendBroadcast');
const dbTablePrefix = process.env.DB_TABLE_PREFIX || '';

const db = new AWS.DynamoDB.DocumentClient({
	endpoint: process.env.DYNAMODB_ENDPOINT || undefined,
});

const queryBroadcastQueueForActiveItemsInPhase = async (phase) => {
	let queue = [];
	let ExclusiveStartKey = null;
	const now = `${Date.now().toString()}.0`;
	do {
		const res = await db
			.query({
				TableName: `${dbTablePrefix}BroadcastQueue`,
				KeyConditionExpression: '#phase = :phase and #runAt <= :now',
				ScanIndexForward: true,
				ExpressionAttributeNames: {
					'#runAt': 'runAt',
					'#phase': 'phase',
				},
				ExpressionAttributeValues: {
					':now': now,
					':phase': phase,
				},
			})
			.promise();
		ExclusiveStartKey = res.LastEvaluatedKey;
		if (Array.isArray(res.Items) && res.Items.length) {
			queue = queue.concat(res.Items);
		}
	} while (ExclusiveStartKey);
	return queue;
};

const getNextQueueItem = async () => {
	const [pendingQueue, inTestQueue] = await Promise.all([
		queryBroadcastQueueForActiveItemsInPhase('pending'),
		queryBroadcastQueueForActiveItemsInPhase('in-test'),
	]);
	const queue = pendingQueue.concat(inTestQueue);
	queue.sort((a, b) => parseFloat(a.runAt) - parseFloat(b.runAt));
	return queue[0] || null;
};

const processBroadcastQueue = async () => {
	try {
		const nextQueueItem = await getNextQueueItem();
		if (nextQueueItem) {
			await sendBroadcast({
				phase: nextQueueItem.phase,
				runAt: nextQueueItem.runAt,
				tags: nextQueueItem.tags,
				excludeTags: nextQueueItem.excludeTags,
				templateId: nextQueueItem.templateId,
				templates: nextQueueItem.templates,
				properties: nextQueueItem.properties,
				interactions: nextQueueItem.interactions,
				interactionWithAnyEmail: nextQueueItem.interactionWithAnyEmail,
				ignoreConfirmed: nextQueueItem.ignoreConfirmed,
				tagOnClick: nextQueueItem.tagOnClick,
				winningType: nextQueueItem.winningType,
			});
		}
		setTimeout(processBroadcastQueue, 20000);
	} catch (err) {
		console.error(err);
	}
};

processBroadcastQueue();
