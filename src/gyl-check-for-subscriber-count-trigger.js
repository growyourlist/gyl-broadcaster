const dynamodb = require('dynopromise-client')
const countSubscribers = require('./gyl-subscriber-counter')
const dbTablePrefix = process.env.DB_TABLE_PREFIX || ''

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

let intervalId = null
const startMonitoring = () => {
	intervalId = setInterval(() => {
		db.get({
			TableName: `${dbTablePrefix}Settings`,
			Key: { settingName: 'previewSubscriberCount' }
		})
		.then(result => {
			if (result && result.Item && result.Item.value.status === 'triggered') {
				clearInterval(intervalId)
				return db
					.put({
						TableName: `${dbTablePrefix}Settings`,
						Item: {
							settingName: 'previewSubscriberCount',
							value: {
								count: 0,
								status: 'processing',
								tags: result.Item.value.tags,
								excludeTags: result.Item.value.excludeTags,
								properties: result.Item.value.properties,
								interactions: result.Item.value.interactions,
								interactionWithAnyEmail:
									result.Item.value.interactionWithAnyEmail,
								ignoreConfirmed: result.Item.value.ignoreConfirmed,
								joinedAfter: result.Item.value.joinedAfter,
							},
						},
					})
					.then(() =>
						countSubscribers({
							tags: result.Item.value.tags,
							excludeTags: result.Item.value.excludeTags,
							properties: result.Item.value.properties,
							interactions: result.Item.value.interactions,
							interactionWithAnyEmail:
								result.Item.value.interactionWithAnyEmail,
							ignoreConfirmed: result.Item.value.ignoreConfirmed,
							joinedAfter: result.Item.value.joinedAfter,
						})
					)
					.then(() => startMonitoring());
			}
		})
		.catch(err => console.error(err))
	}, 3000)
}

startMonitoring()