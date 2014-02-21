wordmaker 词语生成工具
=========
通过词语组成的规律，自动从大文本当中学习得到文本当中的词语，而不再需要其它额外的信息。

很多分词库等都需要字典库，特别在一些专业的领域，需要得到很多的专业相关词语。而人工标注字典需要花很大的时间，所以希望有一个工具能够自动从文本中训练得到词语。分词某类人的用词特点，也可以有所应用。

代码实现
========
最初尝试实现了一个[简单版本](https://github.com/jannson/yaha/blob/master/extra/segword.cpp)，但基于单线程，运行速度慢，并且还消耗巨大的内存。最近尝试接触c++ 11，并吸收了hadoop的map/reduce思想，所以决定拿这个项目练练手，也希望能有人多交流。思想如下：

* 基于节约内存的 Trie树结构
* 用多个线程独立计算各个文本块的词的信息，再循序合并，再用多线程单独计算各个段的词的概率，左右熵，得到词语输出。 
* src/wordmaker.cpp 将在内存当中完成所有计算，并只依赖于cedar.h文件。能够很快处理20M左右的文本。
* src/hugemaker.cpp 
为了节约内存，在计算、合并过程中将产生部分中间文件。可以处理更大的文本(50M+)。marisa:Trie支持mmap将trie文件mmap到内存当中，经过修改完成可以处理更巨大的文本（当时用的时间也会更久）。
* 默认启用4个线程，可以修改代码的宏，也可以自己加命令行。
* 为了代码的简单，只支持gbk编码。
* 在linux/cygwin下编码成功，visual studio下应该也没问题吧。
* 因工作关系用C比C++时间多得多，希望使用代码的人看到代码有任何不符合现在c++观点的作法及时指出，以做交流！

编译与使用
==========
mkdir build

cd build

cmake28 ..

make

./bin/wordmaker input.txt output.txt  或者

./bin/hugemaker input.txt output.txt

linux下可以使用：

iconv -f "gbk" -t "utf-8//IGNORE" < infile > outfile

进行编码转换。windows下当然可以使用notepad++了，转换成ANSI。

测试语料
========
sogou的新闻语料，把各个文本合成在一起总共50M：http://pan.baidu.com/s/1mgoIPxY
里面还有莫言的文集当做输入语料，方法大家测试代码。

这里是我运行的[表现结果](https://github.com/jannson/wordmaker/tree/master/tests)
