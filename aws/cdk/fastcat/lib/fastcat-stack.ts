import * as cdk from 'aws-cdk-lib';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { NetworkStack } from './network';
import { BatchStack, BatchStackProps } from './batch';
import { EventStack } from './event';


interface FastcatStackProps
extends cdk.StackProps, Omit<BatchStackProps, 'vpc'> {
    triggering_suffix: string,
};

export class FastcatStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: FastcatStackProps) {
        super(scope, id, props);

        const node_path = this.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        const network_stack = new NetworkStack(this, 'network', props);
        const batch_stack = new BatchStack(this, 'batch', {
                ... props,
            vpc: network_stack.vpc,
        });

        const dead_letter_queue = new sqs.Queue(this, 'dead-letter-queue', {
            queueName: `${node_path}-dead-letters-${addr8}`,
        });

        const event_stack = new EventStack(this, 'event', {
            dead_letter_queue,
            triggering_s3: {
                bucket: props.s3_configs.in_bucket,
                key: {
                    prefix: props.s3_configs.in_prefixes,
                    suffix: props.triggering_suffix,
                },
            },
            batch_queue: batch_stack.queue,
            batch_job: batch_stack.metadata_parsing_job,
            job_s3: {
                bucket: props.s3_configs.job_bucket,
                prefix: props.s3_configs.job_prefixes[0],
            },
        });
    }
}
