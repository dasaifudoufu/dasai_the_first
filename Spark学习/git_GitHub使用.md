# 命令汇总

## git基础命令

```properties
git status
	查看仓库的状态
git init
	初始化git仓库:生成.git文件,使一个普通的文件夹成为git仓库
git add
	git add 文件名:将文件添加到临时缓冲区
git commit
	git commit -m "text commit":将文件提交到git仓库
git log
	打印git仓库提交日志
git branch
	查看git仓库的分支情况
	git branch a:添加一个a分支
git checkout a
	切换到a分支
git checkout -b
	git checkout -b b:创建b分支并切换到该分支
git merge a
	将分支a合并到master
git branch -d
	git branch -d a:删除a分支
git tag
	git tag v1.0:为当前分支添加标签


```

## git与github交互

### 第一步:生成本地SSH秘钥

在Git bash黑窗口中输入指令

```properties
ssh-keygen -t rsa
```

完成之后生成两个文件id_rsa(私钥),id_rsa.pub(公钥)

### 第二步:找到本地秘钥文件,将公钥复制粘贴到github

文件所在位置

```properties
linux系统:~/.ssh
mac系统:~/.ssh
windows系统:C:\Users\dasaifudoufu\.ssh
```

进入github主页,点击settings,点击ssh,将id_rsa.pub文本内容全部粘贴复制

### 第三步:Git验证是否成功绑定

```properties
ssh -T git@github.com:测试是否绑定成功
```

### 第三步:提交工作使用的命令

本地git推送到远端github

```properties
git push origin master
```

远端拉取到本地

```properties
git pull origin master
```

第一种提交情况:

```properties
	本地没有git仓库
	第一步:将远端库拉取到本地,得到本地git库文件夹
		- 从远端复制仓库链接
		- 在git bash窗口使用命令:
			get clone https://
	第二步:对该文件夹内容进行修改或者添加
		git add
	第三步:提交到本地git库
		git commit -m "commit md file"
	第四步:推送到远端
		git push origin master
		注意:master为当前所在分支,是什么分支就写什么分支
```

第二种情况:

```properties
	已经进行过初次提交,本地已有git仓库
	第一步:在本地库文件夹下打开git bash,连接远程仓库
		git remote add dasai_the_first https://git@github.com:dasaifudoufu/dasai_the_first.git
	第二步:同步远程仓库与本地仓库
		git pull 仓库名 master
	第二步:对该文件夹内容进行修改或者添加
		git add
	第三步:提交到本地git库
		git commit -m ""
	第四步:推送到远端
		git push origin master
```

注意

```properties
-向远端提交代码时,必须先pull同步本地,再push提交远端
```

官方给的代码

```properties
echo "# dasai_the_first" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin git@github.com:dasaifudoufu/dasai_the_first.git
git push -u origin main
```

报错问题合集

```properties
OpenSSL SSL_read: Connection was reset, errno 10054
解决:修改git设置，解除ssl验证
	git config --global http.sslVerify "false"
```





