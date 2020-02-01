const dynamodb = require('dynopromise-client')
const countSubscribers = require('./gyl-subscriber-counter')

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
			TableName: 'Settings',
			Key: { settingName: 'previewSubscriberCount' }
		})
		.then(result => {
			if (result && result.Item && result.Item.value.status === 'triggered') {
				clearInterval(intervalId)
				return db.put({
					TableName: 'Settings',
					Item: {
						settingName: 'previewSubscriberCount',
						value: {
							count: 0,
							status: 'processing',
							tags: result.Item.value.tags,
							excludeTags: result.Item.value.excludeTags,
							properties: result.Item.value.properties,
							interactions: result.Item.value.interactions,
						}
					}
				})
				.then(() => countSubscribers({
					tags: result.Item.value.tags,
					excludeTags: result.Item.value.excludeTags,
					properties: result.Item.value.properties,
					interactions: result.Item.value.interactions,
				}))
				.then(() => startMonitoring())
			}
		})
		.catch(err => console.error(err))
	}, 1000)
}

startMonitoring()