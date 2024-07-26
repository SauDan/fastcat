import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { NetworkStack } from './network';
import { BatchStack, BatchStackProps } from './batch';


interface FastcatStackProps extends cdk.StackProps, Omit<BatchStackProps, 'vpc'> {
};

export class FastcatStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: FastcatStackProps) {
        super(scope, id, props);

        const network_stack = new NetworkStack(this, 'network', props);
        const batch_stack = new BatchStack(this, 'batch', {
                ... props,
            vpc: network_stack.vpc,
        });
    }
}
