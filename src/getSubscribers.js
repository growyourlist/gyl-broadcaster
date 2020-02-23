const dynamodb = require('dynopromise-client')
const dbTablePrefix = process.env.DB_TABLE_PREFIX || ''

// Create DB connection
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
 * Gets all the subscribers from the database who are confirmed and have not
 * unsubscribed.
 * @return {Promise}
 */
const getSubscribersFromDb = (opts = {}, subscriberPool = [],
		LastEvaluatedKey = null) => {
	const scanParams = {
		TableName: `${dbTablePrefix}Subscribers`,
		ConsistentRead: true,
		FilterExpression: '(#conf <> :false1) '
		+ 'and (#unsub = :false2 or attribute_not_exists(#unsub)) '
		+ 'and (not (contains(#tags, :unsubscribed)))',
		ExpressionAttributeNames: {
			'#conf': 'confirmed',
			'#unsub': 'unsubscribed',
			'#tags': 'tags',
		},
		ExpressionAttributeValues: {
			':false1': false,
			':false2': false,
			':unsubscribed': 'unsubscribed',
		}
	}
	if (Object.prototype.hasOwnProperty.call(opts, "consistentRead")) {
		scanParams.ConsistentRead = opts.consistentRead
	}
	if (LastEvaluatedKey) {
		scanParams.ExclusiveStartKey = LastEvaluatedKey
	}
	
	if (Array.isArray(opts.tags)) {
		scanParams['FilterExpression'] += opts.tags.map((tag, index) => 
				` and contains(#tags, :tag${index})`).join('')
		opts.tags.forEach((tag, index) => {
			scanParams['ExpressionAttributeValues'][`:tag${index}`] = tag
		})
	}

	if (Array.isArray(opts.excludeTags)) {
		scanParams['FilterExpression'] += opts.excludeTags.map((tag, index) => 
				` and not(contains(#tags, :excludeTag${index}))`).join('')
		opts.excludeTags.forEach((tag, index) => {
			scanParams['ExpressionAttributeValues'][`:excludeTag${index}`] = tag
		})
	}
	
	if (typeof opts.properties === 'object') {
		let index = 0
		for (let prop in opts.properties) {
			scanParams['FilterExpression'] += ` and #prop${index} = :prop${index}`
			scanParams['ExpressionAttributeNames'][`#prop${index}`] = prop
			scanParams['ExpressionAttributeValues'][`:prop${index}`] = 
				opts.properties[prop]
			index++
		}
	}

	return db.scan(scanParams)
	.then(results => {
		const subscriberBatch = results.Items
		if (opts.onEachSubscriber) {
			subscriberBatch.forEach(subscriber => opts.onEachSubscriber(subscriber))
		}
		const newPool = subscriberPool.concat(subscriberBatch)
		if (!results.LastEvaluatedKey) {
			return newPool
		}
		return getSubscribersFromDb(opts, newPool, results.LastEvaluatedKey)
	})
}

const getSubscribersIdsMatchingInteractionFilter = (
	interaction, subscriberIds = {}, lastEvaluatedKey = null, errorCount = 0
) => {
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
		}
	}
	if (lastEvaluatedKey) {
		queryParams.ExclusiveStartKey = lastEvaluatedKey
	}
	if (typeof interaction.click !== 'undefined') {
		queryParams.ExpressionAttributeNames['#click'] = 'click'
		if (interaction.click) {
			queryParams.FilterExpression += ' and attribute_exists(#click)'
		}
		else {
			queryParams.FilterExpression += ' and attribute_not_exists(#click)'
		}
	}
	if (typeof interaction.open !== 'undefined') {
		queryParams.ExpressionAttributeNames['#open'] = 'open'
		if (interaction.open) {
			queryParams.FilterExpression += ' and attribute_exists(#open)'
		}
		else {
			queryParams.FilterExpression += ' and attribute_not_exists(#open)'
		}
	}
	if (!lastEvaluatedKey) {
		console.log(queryParams)
	}
	return db.query(queryParams)
	.then(result => {
		const newSubscriberIds = Object.assign({}, subscriberIds)
		if (result.Items && result.Items.length) {
			result.Items.forEach(item => {
				newSubscriberIds[item.subscriberId] = true
			})
		}
		if (!result.LastEvaluatedKey) {
			return newSubscriberIds
		}
		return getSubscribersIdsMatchingInteractionFilter(
			interaction,
			newSubscriberIds,
			result.LastEvaluatedKey,
			errorCount
		)
	})
	.catch(err => {
		console.log('encountered error ' + err.message)
		if (err.retryable && errorCount < 50 && !err.bubbling) {
			return getSubscribersIdsMatchingInteractionFilter(
				interaction, subscriberIds, lastEvaluatedKey, (errorCount + 1)
			)
		}
		err.bubbling = true
		throw err
	})
}

const getSubscribersIdsMatchingInteractionFilters = interactions => {
	return Promise.all(interactions.map(
		interaction => getSubscribersIdsMatchingInteractionFilter(interaction)
	))
	.then(results => {
		if (results.length < 2) {
			return Object.assign({}, ...results)
		}
		const subscriberIdsMap = {}
		results.forEach((result, i) => Object.keys(result).forEach(subId => {
			// if the subscriber id is in all matching filters, add it to the map
			for (let j = 0; j < results.length; j++) {
				// we the subId is in index i
				if (j === i) {
					continue
				}
				// at the first sign at the subscriber does not exist in one of the
				// subsets, abort
				if (!results[j][subId]) {
					return
				}
			}
			// If the subId is all of the subsets, i.e. they match all interaction
			// filters, then add them to the map.
			subscriberIdsMap[subId] = true
		}));
		return subscriberIdsMap
	})
}

const getSubscribers = opts => {
	console.log(opts)
	return Promise.all([
		Promise.resolve(opts.interactions && opts.interactions.length &&
			getSubscribersIdsMatchingInteractionFilters(
				opts.interactions
			)
		),
		getSubscribersFromDb(opts)
	])
	.then(results => {
		// No interaction filter was used, so return all subscribers
		console.log(`Sub ids: ${Object.keys(results[0] || {}).length}`)
		if (!opts.interactions || !opts.interactions.length) {
			return results[1]
		}

		// An interaction filter was used, so only return the subscribers whose
		// id was found when querying the interactions
		const subscriberIds = results[0] // Ids of subscribers matching interaction
		const subscriberPool = results[1]
		const finalSubscribers = []
		let subscriber = subscriberPool.pop()
		while (subscriber) {
			if (subscriberIds[subscriber.subscriberId]) {
				finalSubscribers.push(subscriber)
			}
			subscriber = subscriberPool.pop()
		}
		return finalSubscribers
	})
}

module.exports = getSubscribers
