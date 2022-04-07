sam package \
  --region <TYPE_THE_REGION> \
  --template-file ./infra-as-code/template.yaml \
  --output-template-file ./infra-as-code/packaged.yaml \
  --s3-bucket <TYPE_THE_BUCKET_NAME_TO_STORE_THE_ARTIFACTS> \
  --profile <TYPE_THE_PROFILE_NAME>


sam deploy \
    --region <TYPE_THE_REGION> \
    --template-file ./infra-as-code/packaged.yaml \
    --stack-name <TYPE_THE_STACK_NAME> \
    --capabilities CAPABILITY_NAMED_IAM \
    --profile <TYPE_THE_PROFILE_NAME>