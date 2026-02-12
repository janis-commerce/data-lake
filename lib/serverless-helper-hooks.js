/* eslint-disable no-template-curly-in-string */

'use strict';

const Settings = require('@janiscommerce/settings');

const titleCase = require('./helpers/title-case');
const kebabCase = require('./helpers/kebab-case');

const lambdaResource = 'arn:aws:lambda:${aws:region}:${aws:accountId}:function:${self:service}-${self:custom.stage}-DataLakeLoad';

const queueName = 'dataLakeSync';

module.exports = SQSHelper => {

	const dataLakeSettings = Settings.get('dataLake');

	if(!dataLakeSettings?.entities)
		throw new Error('dataLake.entities is required in Settings file');

	return [

		// Lambda Function

		['function', {
			functionName: 'DataLakeLoad',
			handler: 'src/lambda/DataLakeLoad/index.handler',
			description: 'Load data into data lake',
			timeout: 600,
			rawProperties: {
				environment: { ...SQSHelper.getEnvVar(queueName) }
			}
		}],

		// SQS Hooks

		SQSHelper.sqsPermissions,

		...SQSHelper.buildHooks({
			name: queueName,
			consumerProperties: {
				batchSize: 1,
				maximumBatchingWindow: 10,
				eventProperties: {
					maximumConcurrency: 4
				},
				timeout: 900,
				rawProperties: {
					environment: {
						S3_DATA_LAKE_RAW_BUCKET: 'janis-data-lake-service-raw-${self:custom.stage}'
					}
				}
			},
			mainQueueProperties: {
				visibilityTimeout: 1000,
				maxReceiveCount: 2
			}
		}),

		// Create schedule group
		['resource', {
			name: 'DataLakeSyncScheduleGroup',
			resource: {
				Type: 'AWS::Scheduler::ScheduleGroup',
				Properties: {
					Name: 'DataLakeSync'
				}
			}
		}],

		// Create schedule execution role
		['resource', {
			name: 'DataLakeLoadRole',
			resource: {
				Type: 'AWS::IAM::Role',
				Properties: {
					AssumeRolePolicyDocument: {
						Version: '2012-10-17',
						Statement: [{
							Effect: 'Allow',
							Principal: {
								Service: 'scheduler.amazonaws.com'
							},
							Action: 'sts:AssumeRole',
							Condition: {
								StringEquals: { 'aws:SourceAccount': '${aws:accountId}' }
							}
						}]
					},
					Policies: [{
						PolicyName: 'DataLakeLoadExecutionPolicy',
						PolicyDocument: {
							Version: '2012-10-17',
							Statement: [{
								Effect: 'Allow',
								Action: 'lambda:InvokeFunction',
								Resource: lambdaResource
							}]
						}
					}]
				}
			}
		}],

		...dataLakeSettings.entities.map(({ name, frequency }) => ['resource', {
			name: `DataLakeSync${titleCase(name)}Schedule`,
			resource: {
				Type: 'AWS::Scheduler::Schedule',
				Properties: {
					Name: `DataLakeSync${titleCase(name)}Schedule`,
					GroupName: { Ref: 'DataLakeSyncScheduleGroup' },
					Description: `Loads ${name}`,
					FlexibleTimeWindow: {
						Mode: 'FLEXIBLE',
						MaximumWindowInMinutes: 5
					},
					ScheduleExpression: `rate(${frequency || 60} minutes)`,
					State: 'ENABLED',
					Target: {
						Arn: lambdaResource,
						Input: `{"body":{"incremental":true,"entity":"${kebabCase(name)}"}}`,
						RoleArn: {
							'Fn::GetAtt': ['DataLakeLoadRole', 'Arn']
						}
					}
				}
			}
		}])
	];
};
