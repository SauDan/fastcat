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

const s3_configs = {
    in_bucket: "hkgi-fastq-test2",  in_prefixes: [ "upload/HKGI-test", "dev" ],
    job_bucket: "hkgi-fastq-test2",  job_prefixes: [ "dev/jobs" ],
    out_bucket: "hkgi-fastq-test2", out_prefixes: [ "outputs" ],
};


new FastcatStack(stage, 'main', {
    env,
    image: {
        name: "fastcat",
        tag: "latest",
    },
    nodejs_image: {
        name: "fastcat-nodejs",
        tag: "latest",
    },
    s3_configs,
    triggering_suffix: "/fastq_list.csv",
});


const tags = cdk.Tags.of(stage);
tags.add('projectName', 'fastcat');
tags.add('Owner', 'Sau Dan LEE');
tags.add('Application', top.node.id);
tags.add('Stage', stage.node.id);


//end
