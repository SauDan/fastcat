#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { FastcatStack } from '../lib/fastcat-stack';

const app = new cdk.App();
const top = new cdk.Stage(app, 'fastcat');
const stage_name = process.env.STAGE || 'dev';
const stage = new cdk.Stage(top, stage_name);

const env = stage_name == 'dev'? {
    account: "424075490046",
    region: "ap-east-1",
} : undefined;


new FastcatStack(stage, 'main', {
    env,
    image: {
        name: "fastcat",
        tag: "latest",
    }
});


const tags = cdk.Tags.of(stage);
tags.add('projectName', 'fastcat');
tags.add('Owner', 'Sau Dan LEE');
tags.add('Application', top.node.id);
tags.add('Stage', stage.node.id);


//end
