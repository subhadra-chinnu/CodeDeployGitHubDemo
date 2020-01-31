import boto3
import subprocess as cmd
import os
s3 = boto3.resource('s3')

#cp = cmd.run("git clone https://github.com/subhadra-chinnu/CodeDeployGitHubDemo.git ", check=True, shell=True)

for obj in s3.Bucket('med-av-daas-preprod-ca').objects.filter(Prefix = "aws-glue-scripts-ca/functions/"):
    if obj.key.endswith('.py'):
        
        print(obj.key)
        sf = obj.key.split('/')
        sp = sf[2]
        print(sp)
        po = 'geagp_ia_ca.cell_oper_lkp_refresh.py'
        #po = "C:\\Users\\503137452\\Desktop\\aws\\CodeDeployGitHubDemo\\AWS"
        s3.Bucket('med-av-daas-preprod-ca').download_file('aws-glue-scripts-ca/functions/geagp_ia_ca.cell_oper_lkp_refresh.py', po)

'''cp = cmd.run("git add .", check=True, shell=True)
print(cp)

response = input("Do you want to use the default message for this commit?([y]/n)\n")
message = "update the repository"

if response.startswith('n'):
    message = input("What message you want?\n")

cp = cmd.run(f"git commit -a -m '{message}'", check=True, shell=True)
#cp = cmd.run("git remote add origin git@github.com:subhadra-chinnu/CodeDeployGitHubDemo.git", check=True, shell=True)
cp = cmd.run("git push -u origin master -f", check=True, shell=True)'''


