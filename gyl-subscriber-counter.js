require('dotenv').config()
const dynamodb = require('dynopromise-client')
const getSubscribers = require('./src/getSubscribers')

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
 * Updates the `previewSubscriberCount` value in the database.
 * @param {number} countValue 
 */
const updateCountValue = countValue => db.put({
	TableName: 'Settings',
	Item: {
		settingName: 'previewSubscriberCount',
		value: countValue,
	}
})
.catch(err => console.error(err))

/**
 * Count the subscribers matching the filters given in the opts.
 * opts.tags string[] tags that the subscribers should have.
 * opts.properties object property key/value pairs that the subscribers should have.
 * opts.interactions object[] 
 * @param {object} opts
 * @returns {Promise}
 */
const countSubscribers = async (opts = {}) => {
	let count = 0

	// Use an interval to update the count value in the database every second.
	let intervalId = setInterval(() => updateCountValue({
		count,
		status: 'processing'
	}), 1000)

	try {

		// Get all subscribers matching the filters
		const subscribers = await getSubscribers({
			consistentRead: false,
			tags: opts.tags || [],
			excludeTags: opts.excludeTags || [],
			properties: opts.properties || {},
			interactions: opts.interactions || [],
			onEachSubscriber: () => ++count,
		})
		clearInterval(intervalId)

		// Return the final subscriber count.
		await updateCountValue({
			count: subscribers.length,
			status: 'complete',
			tags: opts.tags,
		})
	}
	catch (err) {

		// Update the status of the count with the error.
		clearInterval(intervalId)
		await updateCountValue({
			count,
			status: 'error',
			errorMessage: err.message,
			tags: opts.tags
		})
	}
}

module.exports = countSubscribers
