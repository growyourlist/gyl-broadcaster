require('dotenv').config();
const dynamodb = require('dynopromise-client');
const getSubscribers = require('./getSubscribers');
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
const db = dynamodb(dbParams);

/**
 * Updates the `previewSubscriberCount` value in the database.
 * @param {number} countValue
 */
const updateCountValue = (countValue) =>
	db
		.put({
			TableName: `${dbTablePrefix}Settings`,
			Item: {
				settingName: 'previewSubscriberCount',
				value: countValue,
			},
		})
		.catch((err) => console.error(err));

/**
 * Count the subscribers matching the filters given in the opts.
 * opts.tags string[] tags that the subscribers should have.
 * opts.properties object property key/value pairs that the subscribers should have.
 * opts.interactions object[]
 * @param {object} opts
 * @returns {Promise}
 */
const countSubscribers = async (opts = {}) => {
	let count = 0;
	const fullOpts = {
		tags: opts.tags || [],
		excludeTags: opts.excludeTags || [],
		properties: opts.properties || {},
		interactions: opts.interactions || [],
		interactionWithAnyEmail: opts.interactionWithAnyEmail,
		ignoreConfirmed: opts.ignoreConfirmed,
		joinedAfter: opts.joinedAfter,
	};

	// Use an interval to update the count value in the database every second.
	let intervalId = setInterval(
		() =>
			updateCountValue({
				count,
				status: 'processing',
			}),
		1000
	);

	try {
		// Get all subscribers matching the filters
		const subscribers = await getSubscribers(
			Object.assign({}, fullOpts, {
				consistentRead: false,
				onEachSubscriber: () => ++count,
			})
		);
		clearInterval(intervalId);

		// Return the final subscriber count.
		await updateCountValue(Object.assign({}, fullOpts, {
			count: subscribers.length,
			status: 'complete',
		}));
	} catch (err) {
		// Update the status of the count with the error.
		console.error(err)
		clearInterval(intervalId);
		await updateCountValue(Object.assign({}, fullOpts, {
			count,
			status: 'error',
			errorMessage: err.message,
		}));
	}
};

module.exports = countSubscribers;
