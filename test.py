import subprocess as cmd

cp = cmd.run("git add .", check=True, shell=True)
print(cp)

response = input("Do you want to use the default message for this commit?([y]/n)\n")
message = "update the repository"

if response.startswith('n'):
    message = input("What message you want?\n")

cp = cmd.run(f"git commit -a -m '{message}'", check=True, shell=True)
cp = cmd.run("git remote add origin git@github.com:subhadra-chinnu/CodeDeployGitHubDemo.git", check=True, shell=True)
cp = cmd.run("git push -u origin master -f", check=True, shell=True)


