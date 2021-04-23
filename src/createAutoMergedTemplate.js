const AWS = require('aws-sdk');
const ses = new AWS.SES();

async function createAutoMergedTemplate(params) {
	const {
		subjectTemplateName,
		contentTemplateName,
	} = params;
	const subjectTemplate = await ses.getTemplate({
		TemplateName: subjectTemplateName,
	}).promise()
	const contentTemplate = await ses.getTemplate({
		TemplateName: contentTemplateName,
	}).promise()
	const now = new Date();
	const twoPad = '00';
	const mergedTemplateName = `AutoMerge${
		now.getFullYear()
	}${
		(twoPad + (now.getMonth() + 1)).slice(-2)
	}${
		(twoPad + (now.getDate())).slice(-2)
	}${
		(twoPad + (now.getHours())).slice(-2)
	}${
		(twoPad + (now.getMinutes())).slice(-2)
	}${
		(twoPad + (now.getSeconds())).slice(-2)
	}`
	await ses.createTemplate({
		Template: {
			TemplateName: mergedTemplateName,
			HtmlPart: contentTemplate.Template.HtmlPart,
			TextPart: contentTemplate.Template.TextPart,
			SubjectPart: subjectTemplate.Template.SubjectPart,
		}
	}).promise();
	return mergedTemplateName;
}

module.exports = createAutoMergedTemplate
