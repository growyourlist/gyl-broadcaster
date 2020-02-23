const dynamodb = require('dynopromise-client')
const dbTablePrefix = process.env.DB_TABLE_PREFIX || ''

const debugLog = require('./debugLog')
const getSubscribers = require('./getSubscribers')

const dbParams = {
	region: process.env.AWS_REGION,
}
if (process.env.DYNAMODB_ENDPOINT) {
	dbParams.endpoint = process.env.DYNAMODB_ENDPOINT
}
else {
	dbParams.accessKeyId = process.env.AWS_ACCESS_KEY_ID
	dbParams.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
}

const db = dynamodb(dbParams)

/**
 * Creates a new queue item.
 */
const newQueueItem = (itemData, runAt = Date.now()) => {
	const runAtModified = `${runAt}${Math.random().toString().substring(1)}`
	return Object.assign({}, itemData, {
		queuePlacement: 'queued',
		runAtModified: runAtModified,
		runAt,
		attempts: 0,
		failed: false,
		completed: false,
	})
}

const lockQueue = () => {
	return db.put({
		TableName: 'Settings',
		Item: {
			'settingName': 'isDoingBroadcast',
			'value': true,
		}
	})
}

const unlockQueue = () => {
	return db.put({
		TableName: 'Settings',
		Item: {
			'settingName': 'isDoingBroadcast',
			'value': false,
		}
	})
}

const validateConditions = () => {
	return db.get({
		TableName: 'Settings',
		ConsistentRead: true,
		Key: { 'settingName': 'isDoingBroadcast' }
	})
	.then(res => {
		if (res && res.Item && res.Item.value === true) {
			throw new Error('Queue is currently locked')
		}
		return true
	})
}

const processBatches = (batches, emailCounter = 0) => new Promise(resolve => {
	if (batches.length < 1) {
		return resolve(emailCounter)
	}
	debugLog(`${(new Date).toISOString()}: Batch count: ${batches.length}`)
	const nextBatch = batches.pop()
	return db.batchWrite({ RequestItems: { [`${dbTablePrefix}Queue`]: nextBatch } })
	.then(result => {
		const unprocessedItems = result.UnprocessedItems
		&& result.UnprocessedItems[`${dbTablePrefix}Queue`]
		if (unprocessedItems && Array.isArray(unprocessedItems)) {
			emailCounter += (nextBatch.length - unprocessedItems.length)
			debugLog(`${(new Date).toISOString()}: Requeuing `
			+ `${unprocessedItems.length} items`)
			batches.push(unprocessedItems)
		}
		else {
			emailCounter += nextBatch.length
		}
		return setTimeout(
			() => resolve(processBatches(batches, emailCounter)),
			Math.random() * 500
		)
	})
	.catch(err => {
		if (err.name === 'ProvisionedThroughputExceededException') {
			console.log(`${(new Date).toISOString()}: Requeuing items after `
			+ 'throughput exceeded')
			batches.push(nextBatch)
		}
		else {
			console.log(`${(new Date).toISOString()}: Failed batch: ${err.message}`)
		}
		return setTimeout(
			() => resolve(processBatches(batches, emailCounter)),
			Math.random() * 2000
		)
	})
})

/**
 * Sends a broadcast email to all applicable subscribers.
 * @param  {Object} broadcastData Defines the broadcast to send.
 * @return {Promise}
 */
const sendBroadcast = broadcastData => validateConditions()
.then(() => lockQueue())
.then(() => getSubscribers({
	tags: broadcastData.tags,
	excludeTags: broadcastData.excludeTags,
	properties: broadcastData.properties,
	interactions: broadcastData.interactions,
}))
.then(subscribers => {
	if (!subscribers || !subscribers.length) {
		debugLog(`${(new Date).toISOString()}: No subscribers found for broadcast`)
		return 0
	}
	const putRequests = subscribers.map(subscriber => ({
		PutRequest: {
			Item: newQueueItem(
				{
					type: 'send email',
					subscriber: subscriber,
					subscriberId: subscriber.subscriberId,
					templateId: broadcastData.templateId,
					tagReason: broadcastData.tags,
				},
				Date.now() + (broadcastData.runIn || 0)
			)
		}
	}))

	// Batch the put requests for efficient writing to the db.
	let currentBatch = []
	const batches = []
	const batchThreshold = 25
	putRequests.forEach(putRequest => {
		if (currentBatch.length === batchThreshold) {
			batches.push(currentBatch)
			currentBatch = []
		}
		currentBatch.push(putRequest)
	})
	batches.push(currentBatch)
	return processBatches(batches)
})
.then(emailCount => {
	if (emailCount) {
		console.log(`${(new Date).toISOString()}: Added `
		+ `${emailCount} email/s to the queue`)
	}
})
.catch(err => console.log(`${(new Date).toISOString()}: Error while sending `
+ `broadcast: ${err.message}`))
.then(() => unlockQueue())

module.exports = sendBroadcast
