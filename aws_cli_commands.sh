 
 #create policy using the json file in the repository
 aws iam create-policy --policy-name my-policy-mwaa-cli-test --policy-document file://policy.json
 
  #Attach policy to the role created
 aws iam attach-role-policy --policy-arn  "replace policy arn created in step one"   --role-name  "replace role name in your environment"
