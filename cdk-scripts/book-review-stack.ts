/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  SPDX-License-Identifier: MIT-0

  Permission is hereby granted, free of charge, to any person obtaining a copy of this
  software and associated documentation files (the "Software"), to deal in the Software
  without restriction, including without limitation the rights to use, copy, modify,
  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
   SPDX-License-Identifier: MIT
*/

import {Duration, Stack, StackProps, Size, RemovalPolicy} from 'aws-cdk-lib';
import {Function,Runtime, Code, LayerVersion, Architecture} from 'aws-cdk-lib/aws-lambda';
import {Policy, Role, PolicyStatement, ServicePrincipal, ManagedPolicy, CompositePrincipal, User, ArnPrincipal, PolicyDocument} from 'aws-cdk-lib/aws-iam';
import {Vpc, SecurityGroup, InterfaceVpcEndpoint, InterfaceVpcEndpointService, SubnetType, Peer, Port, CfnVPCEndpoint} from 'aws-cdk-lib/aws-ec2';
import {Construct} from 'constructs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as s3 from 'aws-cdk-lib/aws-s3';
import {config as config} from "./config";
import path = require('path');
import { Pass, Choice } from 'aws-cdk-lib/aws-stepfunctions';
import { Bucket, BlockPublicAccess } from 'aws-cdk-lib/aws-s3';


export class BookReviewStack extends Stack{
    constructor(scope: Construct, id: string, props: StackProps){
        super(scope, id, props);
        //const region = process.env.deploy_region;
        const env_context = this.node.tryGetContext(process.env.CDK_DEFAULT_REGION as string);

        const vpcID = env_context['VpcID'];
        const dbSecretName = env_context['DBSecretName'];
        //const auroraSGID = env_context['AuroraSecurityGroupID'];
        //Bring in existing vpc
        const ivpc = Vpc.fromLookup(this, "reviews-vpc", {
            vpcId: vpcID
        });
        const vpc = ivpc as Vpc;

        const reviewsBucket = this.createReviewsBucket(process.env.CDK_DEFAULT_ACCOUNT, process.env.CDK_DEFAULT_REGION);

        let roleName: string = "bookreview-lambda-role-" + process.env.CDK_DEFAULT_REGION
        const lambdaRole = this.createLambdaRole(roleName, reviewsBucket.bucketName, dbSecretName, process.env.CDK_DEFAULT_REGION as string, process.env.CDK_DEFAULT_ACCOUNT as string);

        //Create VPC endpoints
        const endpointSG = this.createVPCEndpointSecurityGroup(vpc);
        this.createVPCEndpointForCWLogs(vpc, endpointSG);
        this.createVPCEndpointForSecrets(vpc, endpointSG);
        this.createVpcEndPointForS3(vpc);


        //Create reviews pre-process lambda
        const bookReviewPreProcessLambda = new Function(this, "book-reviews-preprocess-lambda", {
            functionName: "book-reviews-preprocess-lambda",
            runtime: Runtime.PYTHON_3_9,
            handler: "bookreview_orchestrator.lambda_handler",
            memorySize: 512,
            timeout: Duration.minutes(10),
            environment: {
                "BOOK_REVIEW_BUCKET": reviewsBucket.bucketName,
                "BOOK_REVIEW_BUCKET_PREFIX": config.S3ReviewsPendingPrefix,
                "BOOK_REVIEW_BUCKET_PREFIX_COMPLETED": config.S3ReviewsCompletedPrefix,
                "CHUNK_SIZE": "40"
            },
            code: Code.fromAsset(path.join(__dirname, '../lambda/bookreview_orchestrator/app')),
            role: lambdaRole
        });

        //Create reviews post-process lambda
        const bookReviewPostProcessLambda = new Function(this, "book-reviews-postprocess-lambda", {
            functionName: "book-reviews-postprocess-lambda",
            runtime: Runtime.PYTHON_3_9,
            handler: "bookreview_process_complete.lambda_handler",
            memorySize: 512,
            environment: {
                "BOOK_REVIEW_BUCKET": reviewsBucket.bucketName,
                "BOOK_REVIEW_BUCKET_PREFIX": config.S3ReviewsPendingPrefix,
                "BOOK_REVIEW_BUCKET_PREFIX_COMPLETED": config.S3ReviewsCompletedPrefix,
            },
            timeout: Duration.minutes(10),
            code: Code.fromAsset(path.join(__dirname, '../lambda/bookreview_process_complete_step/app')),
            role: lambdaRole
        });

        // Creating the Lambda Layer
        const pysqlLayer = new LayerVersion(this, 'PYMySQLLayer', {
            compatibleRuntimes: [ Runtime.PYTHON_3_9 ],
            compatibleArchitectures: [ Architecture.X86_64 ],
            code: Code.fromAsset(path.join(__dirname, '../layer/PYMySQLLayer.zip'))
        });



        const sgDBProcessingLambda = new SecurityGroup(this, 'LambdaSecurityGroup', {
                   vpc,
                   allowAllOutbound: true,
                });

        //const auroaSecurityGroup = SecurityGroup.fromSecurityGroupId(this, 'aurora-sg', auroraSGID, {
        //        mutable: true
        //        });
        //auroaSecurityGroup.addIngressRule( sgDBProcessingLambda, Port.allTraffic(), 'Allow TCP traffic within VPC');

        //Create reviews post-process lambda
        const bookReviewDBProcessingLambda = new Function(this, "book-reviews-dbprocessing-lambda", {
            functionName: "book-reviews-dbprocessing-lambda",
            runtime: Runtime.PYTHON_3_9,
            handler: "bookreview_db_processing.lambda_handler",
            memorySize: 512,
            environment: {
                "DBSecretName": dbSecretName,
                "DB_NAME": config.DBName
            },
            layers: [pysqlLayer],
            timeout: Duration.minutes(10),
            code: Code.fromAsset(path.join(__dirname, '../lambda/bookreview_db_processing/app')),
            role: lambdaRole,
            securityGroups: [sgDBProcessingLambda],
            vpc: vpc,
            vpcSubnets: { subnetType: SubnetType.PUBLIC },
            allowPublicSubnet: true
        });
        /*
        vpc: vpc,
            vpcSubnets: { subnetType: SubnetType.PUBLIC },
            allowPublicSubnet: true,

        */


        //Define tasks for statemachine
        const preProcessLambdaTask = new tasks.LambdaInvoke(this, 'bookreviews-preprocess-lambda', {
            lambdaFunction: bookReviewPreProcessLambda
        });

        //Define tasks for statemachine
        const dbProcessingLambdaTask = new tasks.LambdaInvoke(this, 'bookreviews-dbprocessing-lambda', {
            lambdaFunction: bookReviewDBProcessingLambda
        });


        const reviewsMap = new sfn.Map(this, 'bookreviews-processing-map', {
            maxConcurrency: 40,
            parameters: {
                "s3Bucket.$": "$$.Map.Item.Value.s3Bucket",
                "prefix.$": "$$.Map.Item.Value.prefix"
            },
            itemsPath: sfn.JsonPath.stringAt("$.Payload.bookReviews"),
            resultSelector: {
                "s3Bucket.$": "$.[*].Payload.s3Bucket",
                "prefix.$": "$.[*].Payload.prefix"
            },
            resultPath: "$.mapOutput"

        });

        const postProcessLambdaTask = new tasks.LambdaInvoke(this, 'bookreviews-postprocess-lambda', {
            inputPath: "$.mapOutput",
            lambdaFunction: bookReviewPostProcessLambda,
            resultSelector: {"payload.$": "$.Payload"},
            resultPath: "$.lambda_out"
        });
        reviewsMap.iterator(dbProcessingLambdaTask);

        const passState = new Pass(this, 'pass-state');

        const choice = new Choice(this, 'bookreviews-has-more-files?')
            .when(sfn.Condition.numberGreaterThan("$.lambda_out.payload.s3_objects", 0), preProcessLambdaTask)
            .otherwise(passState);


        //Create state machine
        const stateMachine = new sfn.StateMachine(this, 'bookreviews-state-machine', {
            stateMachineName: "book-reviews-statemachine",
            definition: preProcessLambdaTask.next(reviewsMap.next(postProcessLambdaTask)).
                next(choice)
        });

    }

    private createReviewsBucket(account?: string, region?: string): Bucket {
        const s3BucketPrefix = "bookreviews-" + account + "-" + region;
        const s3Bucket = new Bucket(this, s3BucketPrefix, {
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true
        });

        return s3Bucket;
    }

    private createLambdaRole(roleName: string, bucketName: string, secretName: string, region: string, account: string): Role {
        const secretArn = `arn:aws:secretsmanager:${region}:${account}:secret:${secretName}*`;

        const lambdaRole = new Role(this, roleName, {
            assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
            roleName: roleName,
            description: "Book review lambda role",
            managedPolicies : [
                      ManagedPolicy.fromAwsManagedPolicyName(`service-role/AWSLambdaVPCAccessExecutionRole`),
                      ManagedPolicy.fromAwsManagedPolicyName(`service-role/AWSLambdaBasicExecutionRole`)
            ]
        });
        const bucketArn = "arn:aws:s3:::" + bucketName;
        const lambdaPolicy = new Policy(this, 'book-review-lambda-policy', {
            policyName: `book-review-lambda-policy`,
            statements: [
            new PolicyStatement({
                resources:
                [
                    bucketArn,
                    bucketArn + '/*'
                ],
                actions: [
                    's3:GetObject',
                    's3:List*',
                    's3:PutObject',
                    's3:DeleteObject',
                    's3:CopyObject'
                ]
            }),
             new PolicyStatement({
                    resources: [secretArn],
                    actions: [
                        "secretsmanager:GetSecretValue"
                     ],
                }),
            ],
        });

        lambdaRole.attachInlinePolicy(lambdaPolicy);
        return lambdaRole;
    }

     private createVPCEndpointForCWLogs(vpc: Vpc, vpcEndpointSG: SecurityGroup): void{
        const vpcEndpointForCWLogs = new InterfaceVpcEndpoint(this, 'cw-endpoint', {
            vpc,
            service: new InterfaceVpcEndpointService(`com.amazonaws.${Stack.of(this).region}.logs`, 443),
            securityGroups: [ vpcEndpointSG ],
            subnets: vpc.selectSubnets({
                subnetType: SubnetType.PUBLIC
             }),
            privateDnsEnabled: true,
        });

        const vpcEndpointForCWLogsPolicyStatement = new PolicyStatement({
            actions: ['*'],
            resources: ['*'],
            principals: [new ArnPrincipal('*')],
         });

         vpcEndpointForCWLogs.addToPolicy(vpcEndpointForCWLogsPolicyStatement);
    }

    private createVPCEndpointForSecrets(vpc: Vpc, vpcEndpointSG: SecurityGroup): void{
        const vpcEndpointForSecrets = new InterfaceVpcEndpoint(this, 'secrets-endpoint', {
            vpc,
            service: new InterfaceVpcEndpointService(`com.amazonaws.${Stack.of(this).region}.secretsmanager`, 443),
            securityGroups: [ vpcEndpointSG ],
            subnets: vpc.selectSubnets({
                subnetType: SubnetType.PUBLIC
             }),
            privateDnsEnabled: true,
        });

        const vpcEndpointForSecretsPolicyStatement = new PolicyStatement({
            actions: ['*'],
            resources: ['*'],
            principals: [new ArnPrincipal('*')],
         });

         vpcEndpointForSecrets.addToPolicy(vpcEndpointForSecretsPolicyStatement);
    }

    private createVpcEndPointForS3(vpc: Vpc) : CfnVPCEndpoint {
        //Get routetables associated with subnets
        let routeTablesAssociated:string[]= new Array();

        vpc.publicSubnets.forEach(({ routeTable: { routeTableId } }, index) => {
            routeTablesAssociated.push(routeTableId);
        });

        var uniqueRouteTables = routeTablesAssociated.filter(function(elem, index, self) {
             return index === self.indexOf(elem);
        })

        const s3EndpointPolicy =  new PolicyDocument({
            statements: [
                new PolicyStatement({
                    actions: ["*"],
                    resources: ["*"],
                    principals: [new ArnPrincipal('*')],
                }),
                new PolicyStatement({
                    resources: ["*"],
                    actions: ["*"],
                    principals: [new ArnPrincipal('*')],
                })
            ]
        })

        const s3VpcEndpoint = new CfnVPCEndpoint(this, 'S3VPCEndpoint', {
            serviceName: `com.amazonaws.${Stack.of(this).region}.s3`,
            vpcId: vpc.vpcId,
            routeTableIds: uniqueRouteTables,
            policyDocument: s3EndpointPolicy,
        });
        return s3VpcEndpoint;
    }

    private createVPCEndpointSecurityGroup(vpc: Vpc): SecurityGroup{
        const vpcEndpointSecurityGroup = new SecurityGroup(this, 'bookreviews-endpoint-sg', {
            vpc: vpc,
            securityGroupName: 'bookreviews-endpoint-sg',
            allowAllOutbound: true,
            description: 'Security group for VPC interface endpoints'
        });

        vpcEndpointSecurityGroup.addIngressRule(Peer.ipv4(vpc.vpcCidrBlock), Port.allTraffic(), 'Allow TCP traffic within VPC');
        return vpcEndpointSecurityGroup;
    }





}