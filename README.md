CMU 15-445 2022FALL 课程笔记
***

## starters

lab课程网站：[课程网站](https://15445.courses.cs.cmu.edu/fall2022)

首先将CMU 15-445的课程代码clone至本地 working dirctory 参考 [set-ups](https://github.com/cmu-db/bustub#readme)

注意因为时间的原因可能 `lab课程网站上lab的要求`和 `现在的的代码`会有一些不同

例如本文跟做的lab是`2022FaLL`，时间是在2023年2月份左右，在做lab 1 的时候所需要的`buffer_pool_manager_instance.h`和`buffer_pool_manager_instance.cpp`已经被更名成为了`buffer_pool_manager.h`，`buffer_pool_manager.cpp`而且内容也进行了更改


我查看了2022 lab课程网站上所有project最后的更新是在`2022-12-01`，追踪对应的git commit至

![last available](https://user-images.githubusercontent.com/116239454/216802842-885ee421-f33d-40e9-a094-2666ebae21fb.png)

*交大校友看过来，这个commit的skyzh就是我们的迟先生*

---


所以在本地的master branch

**reset至lab 代码对应的commit**
`git reset d830931a9b2aca66c0589de67b5d7a5fd2c87a79 --hard`


最后有个很坑的点就是查看你cmake时使用的编译器是不是课程要求的llvm-14，我之前用成GNU11.3.0导致所有的多线程测试直接就过了,尽管我还没有加锁

![image](https://user-images.githubusercontent.com/99662709/234530461-7a5f1f8e-1f60-41da-9e23-85831bf4a9ae.png)


### llvm12安装：
我是自己编译然后加环境变量里面的，先贴一个官网：[https://clang.llvm.org/get_started.html](https://llvm.org/docs/GettingStarted.html#getting-the-source-code-and-building-llvm)

首先安装cmake等必要的东西
`sudo apt-get install build-essential
sudo apt-get install cmake ninja-build`

克隆llvm：
`git clone https://github.com/llvm/llvm-project.git
cd llvm-project`
切换到课程要求的llvm12
`git checkout -b 12.x remotes/origin/release/12.x`

然后按照官网的步骤编译源码，主要是这段有用
![image](https://user-images.githubusercontent.com/99662709/234539957-21ddcb45-799d-426d-b845-20f4369cdc6a.png)

`
cmake -S llvm -B build -G Ninja -DLLVM_ENABLE_PROJECTS='clang;clang-tools-extra;lldb;lld' -DCMAKE_BUILD_TYPE=Release

ninja && sudo ninja install -j4
`

看眼clang和llvm版本
`clang --version`
![image](https://user-images.githubusercontent.com/99662709/234535526-e2b96a58-ecf0-4403-8c00-8223f08042e7.png)
这里正确的version应该是12，因为我一开始15445项目拉的最新的库，当时要求的是llvm14,懒得再重新编译了就直接用的14

添加环境变量
vim ~/.bashrc,在最后加上
`#llvm-14
export C=clang

export CXX=clang++

export LD=lld`

![image](https://user-images.githubusercontent.com/99662709/234539232-37bb8119-3794-4f5d-9764-331e500047b3.png)


最后附加上2022课程的gradescope  [gradescope](https://www.gradescope.com/courses/425272)
课程代码 PXWVR5
学校记得填Carnegie Mellon University
## 项目课程笔记
### project 0 ： Concurrent Trie
实现一个字典树，检查实验者代码水平

### project 1 ：buffer pool manager
将disk上的内容page读取到buffer里，或者将其写回disk
