import * as cdk from 'aws-cdk-lib';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { NetworkStack } from './network';
import { BatchStack, BatchStackProps } from './batch';
import { EventStack } from './event';


interface FastcatStackProps
extends cdk.StackProps, Omit<BatchStackProps, 'vpc' | 's3_configs'> {
    s3_configs: {
        in_bucket: string,  in_prefixes: string[],
        job_bucket: string, job_prefixes: string[],
        out_bucket: string, out_prefixes: string[],
    },
    triggering_suffix: string,
};

export class FastcatStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: FastcatStackProps) {
        super(scope, id, props);

        const node_path = this.node.path.replace(/\//g, '-');
        const addr8 = this.node.addr.substring(0, 8);

        const in_bucket = s3.Bucket.fromBucketName(this, 'in-bucket',
                                                   props.s3_configs.in_bucket);
        const job_bucket = s3.Bucket.fromBucketName(this, 'job-bucket',
                                                   props.s3_configs.job_bucket);
        const out_bucket = s3.Bucket.fromBucketName(this, 'out-bucket',
                                                    props.s3_configs.out_bucket);

        const network_stack = new NetworkStack(this, 'network', props);
        const batch_stack = new BatchStack(this, 'batch', {
                ... props,
            vpc: network_stack.vpc,
            s3_configs: {
                    ... props.s3_configs,
                in_bucket,
                job_bucket,
                out_bucket,
            }
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
