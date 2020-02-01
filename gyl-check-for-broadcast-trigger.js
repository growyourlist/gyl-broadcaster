require('dotenv').config()
const dynamodb = require('dynopromise-client')
const sendBroadcast = require('./src/sendBroadcast')

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
			Key: { settingName: 'pendingBroadcast' }
		})
		.then(result => {
			if (result && result.Item && Array.isArray(result.Item.value.tags) &&
					result.Item.value.templateId && result.Item.value.properties) {
				let runIn = 0
				if (result.Item.value.runAt) {
					runIn = result.Item.value.runAt - Date.now()
				}
				clearInterval(intervalId)
				return db.delete({
					TableName: 'Settings',
					Key: { settingName: 'pendingBroadcast' }
				})
				.then(() => sendBroadcast({
					tags: result.Item.value.tags,
					excludeTags: result.Item.value.excludeTags,
					templateId: result.Item.value.templateId,
					properties: result.Item.value.properties,
					interactions: result.Item.value.interactions,
					runIn,
				}))
				.catch(err => console.error(err))
				.then(() => startMonitoring())
			}
		})
		.catch(err => console.error(err))
	}, 1000)
}

startMonitoring()