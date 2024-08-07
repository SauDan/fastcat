import * as cdk from 'aws-cdk-lib';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as batch from 'aws-cdk-lib/aws-batch';
import { Construct } from 'constructs';

interface EventStackProps extends cdk.NestedStackProps {
    dead_letter_queue: sqs.IQueue,
    triggering_s3: {
        bucket: string,
        key: {
            prefix: string[],
            suffix: string,
        },
    },
    job_s3: {
        bucket: string,
        prefix: string,
    },
    batch_queue: batch.IJobQueue,
    batch_job_first: batch.IJobDefinition,
    batch_job_last_name_prefix: string,
    failure_sns: sns.ITopic,
    success_sns: sns.ITopic,
}

export class EventStack extends cdk.NestedStack {
    constructor(scope: Construct, id: string, props: EventStackProps) {
        super(scope, id, props);

        this.add_start_batch_job(props);
        this.add_batch_job_notification(props);
    }

    private add_start_batch_job(props: EventStackProps) {
        const node_path = this.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        const bucket = events.EventField.fromPath("$.detail.bucket.name");
        const key = events.EventField.fromPath("$.detail.object.key");
        const input_event = {
            Parameters: {
                fastqlist_file_s3_url: `s3://${bucket}/${key}`,
            },
        };

        const rule = new events.Rule(this, 'trigger-fastcat', {
            ruleName: `${node_path}-trigger-${addr8}`,
            eventPattern: {
                source: [ "aws.s3" ],
                detail: {
                    bucket: {
                        name: [ props.triggering_s3.bucket ],
                    },
                    object: {
                        key: props.triggering_s3.key.prefix.map(prefix => {
                            return {
                                wildcard: `${prefix}/\*${props.triggering_s3.key.suffix}`,
                            }
                        }),
                    },
                }
            },
            targets: [
                new targets.BatchJob(props.batch_queue.jobQueueArn,
                                     props.batch_queue,
                                     props.batch_job_first.jobDefinitionArn,
                                     props.batch_job_first, {
                                         deadLetterQueue: props.dead_letter_queue,
                                         event: events.RuleTargetInput.fromObject(input_event),
                                     }),
            ],
        });
    }

    private add_batch_job_notification(props: EventStackProps) {
        const node_path = this.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        const fields = {
            job_name: events.EventField.fromPath("$.detail.jobName"),
            job_arn: events.EventField.fromPath("$.detail.jobArn"),
            job_id: events.EventField.fromPath("$.detail.jobId"),
            job_queue_arn: events.EventField.fromPath("$.detail.jobQueue"),
            job_def_arn: events.EventField.fromPath("$.detail.jobDefinition"),
        };

        const subject = `FAILED fastcat job: ${fields.job_name} (${fields.job_id})`;
        const job_details = `  Name: ${fields.job_name}
  Id: ${fields.job_id}
  Arn: ${fields.job_name}
  Queue: ${fields.job_queue_arn}
  Definition: ${fields.job_def_arn}
`;

        const failure_message = {
            Subject: subject,
            Message: `fastcat job failed.
${job_details}
`,
        };
        const failure = new events.Rule(this, 'fastcat-failure', {
            ruleName: `${node_path}-failure-${addr8}`,
            eventPattern: {
                source: [ "aws.batch" ],
                detail: {
                    jobQueue: [ props.batch_queue.jobQueueArn ],
                    status: [ "FAILED" ],
                }
            },
            targets: [
                new targets.SnsTopic(props.failure_sns, {
                    deadLetterQueue: props.dead_letter_queue,
                    message: events.RuleTargetInput.fromObject(failure_message),
                }),
            ],
        });


        const success_message = {
            Subject: subject,
            Message: `fastcat job ${fields.job_name} succeeded`,
        };
        const success = new events.Rule(this, 'fastcat-success', {
            ruleName: `${node_path}-success-${addr8}`,
            eventPattern: {
                source: [ "aws.batch" ],
                detail: {
                    jobQueue: [ props.batch_queue.jobQueueArn ],
                    jobName: events.Match.prefix(props.batch_job_last_name_prefix),
                    status: [ "SUCCEEDED" ],
                }
            },
            targets: [
                new targets.SnsTopic(props.success_sns, {
                    deadLetterQueue: props.dead_letter_queue,
                    message: events.RuleTargetInput.fromObject(success_message),
                }),
            ],
        });
    }
}
