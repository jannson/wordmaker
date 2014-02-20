/*
 * =====================================================================================
 *
 *       Filename:  cedar.cpp
 *
 *    Description:  ;
 *
 *        Version:  1.0
 *        Created:  2014年02月17日 10时28分09秒
 *       Compiler:  gcc
 *
 *         Author:  Jannson, gandancing@gmail.com
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdint.h>
#include <cstdio>
#include <cstdarg>
#include <cstddef>
#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <limits>
#include <vector>
#include <list>
#include <functional>

#define USE_FAST_LOAD
#include <cedar.h>

using namespace std;

static const size_t NUM_RESULT = 1024;

typedef cedar::da<int> trie_t;
typedef trie_t::result_triple_type result_t;
typedef trie_t::iter_func iter_func_t;

struct ResOper:public iter_func_t
{
	ResOper(trie_t* pt):ptrie(pt), len(0){}
	void operator()(result_t& res)
	{
		char suffix[1024];
		ptrie->suffix(suffix, res.length, res.id);
		//fprintf(stderr, "%d:%d:%s\n", res.value, res.length, suffix);
		fprintf(stderr, "0x%02x\n", suffix[0]);
		len++;

#if 0
		char* word = "我";
		list<result_t> result_triple;
		ptrie->dump(result_triple, NUM_RESULT);
		for (list<result_t>::iterator it = result_triple.begin(); it != result_triple.end(); it++)
		{
			ptrie->suffix(suffix, it->length, it->id);
			fprintf(stderr, "%d:%ld:%ld:%s\n", it->value
					, it->length, it->id, suffix);
		}
#endif
	}
	size_t	len;
	trie_t* ptrie;
};

#define SWP(x,y) (x^=y, y^=x, x^=y)

void strrev_utf8(string& ref_s)
{
  int l = ref_s.length()/2;
  short *q, *p = (short*)&ref_s[0];
  q = p+l;
  for(--q; p < q; ++p, --q) SWP(*p, *q);
}

void test_res(result_t& res)
{
	fprintf(stderr, "ResOper %d\n", __LINE__);
}

//get the range in [start, end)
void gbk_range(int& start, int& end, int pos, int split_n)
{
	//[81~A0] [B0~FE]
	const int total = (0xa0 - 0x80 + 1) + (0xfe - 0xb0 + 1);
	const int range = (total + split_n - 1)/split_n;

	//calc start
	start = 0x80 + pos * range;
	end = 0x80 + (pos + 1) * range;
	if((start <= 0xa0) && (end > 0xa0))
	{
		fprintf(stderr, "start:%02x end:%02x\n", start, end);
		end += 0xb0 - 0xa0;
	}
	else if(start > 0xa0) {
		start += 0xb0 - 0xa0;
		end += 0xb0 - 0xa0;
	}
}

int main(int argc, char** argv)
{
	char* words[] = {"appled", "at", "app", "aword", "awt", "我们", "我"};
	trie_t trie;

	for(int i = 0; i < sizeof(words)/sizeof(words[0]); i++)
	{
        fprintf(stderr, "%s\t%d\n", words[i], i);
		trie.update(words[i], strlen(words[i]), i);
	}

	char test_c[4];
	*((unsigned short*)test_c) = 261;
	test_c[2] = '\0';
	trie.update(test_c, strlen(test_c), 1);
	fprintf(stderr, "testc:%s %d\n", test_c, strlen(test_c));

	trie.update(words[6], strlen(words[6]), 10);

	fprintf(stderr, "keys: %ld\n", trie.num_keys());
	fprintf(stderr, "size: %ld\n", trie.size());
	fprintf(stderr, "nonzero_size: %ld\n", trie.nonzero_size());
	
	char* word = "我";
	char suffix[1024];
	list<result_t> result_triple;

#if 1
	if(const size_t n = trie.commonPrefixPredict(word, result_triple, NUM_RESULT))
	{
		fprintf(stderr, "n is %ld\n", n);
		for (list<result_t>::iterator it = result_triple.begin(); it != result_triple.end(); it++)
		{
			trie.suffix(suffix, it->length, it->id);
			fprintf(stderr, "%d:%ld:%ld:%s%s\n", it->value
					, it->length, it->id, word, suffix);
		}
	}
#endif

#if 0
	trie.dump(result_triple, NUM_RESULT);
	for (result_t::iterator it = result_triple.begin(); it != result_triple.end(); it++)
	{
		trie.suffix(suffix, it->length, it->id);
		fprintf(stderr, "%d:%ld:%ld:%s\n", it->value
				, it->length, it->id, suffix);
	}
#endif

	//fprintf(stderr, "begin dump %d\n", __LINE__);
	//ResOper res(&trie);
	//std::function<void(result_t&)> res = test_res;
	//trie.dump(res, 2, 3);
	//fprintf(stderr, "end dump len=%d %d\n", res.len, __LINE__);

#if 0
	string str_tmp(words[0]);
	strrev_utf8(str_tmp);
	fprintf(stderr, "reverse:%s %d %d %x\n", str_tmp.c_str()
			, strlen(words[0]), str_tmp.length(), words[0][5]);
#endif

#if 0
	int n = 3;
	for(int i = 0; i < n; i++){
		int start, end;
		gbk_range(start, end, i, n);
		fprintf(stderr, "i %d: [0x%02x, 0x%02x)\n", i, start, end);
	}
#endif

	string trie_file("o.txt_buck_0");
	trie_t trie2;
	trie2.open(trie_file.c_str());
	fprintf(stderr, " num keys:%d\n", trie.num_keys());
	ResOper res(&trie2);
	trie2.dump(res);

	return 0;
}

