import * as cdk from 'aws-cdk-lib';
import * as sqs from 'aws-cdk-lib/aws-sqs';
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
    batch_job: batch.IJobDefinition,
}

export class EventStack extends cdk.NestedStack {
    constructor(scope: Construct, id: string, props: EventStackProps) {
        super(scope, id, props);

        const node_path = this.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);


        const bucket = events.EventField.fromPath("$.detail.bucket.name");
        const key = events.EventField.fromPath("$.detail.object.key");
        const input_event = {
            Parameters: {
                metadata_file_s3_url: `s3://${bucket}/${key}`,
                output_dir_s3_url: `s3://${props.job_s3.bucket}/${props.job_s3.prefix}`,
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
                                     props.batch_job.jobDefinitionArn,
                                     props.batch_job, {
                                         deadLetterQueue: props.dead_letter_queue,
                                         event: events.RuleTargetInput.fromObject(input_event),
                                     }),
            ],
        });
    }
}
