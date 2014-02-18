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
		fprintf(stderr, "%d:%d:%s\n", res.value, res.length, suffix);
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

int main(int argc, char** argv)
{
	char* words[] = {"appled", "at", "app", "aword", "awt", "我们", "我"};
	trie_t trie;

	for(int i = 0; i < sizeof(words)/sizeof(words[0]); i++)
	{
        fprintf(stderr, "%s\t%d\n", words[i], i);
		trie.update(words[i], strlen(words[i]), i);
	}

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

	string str_tmp(words[0]);
	strrev_utf8(str_tmp);
	fprintf(stderr, "reverse:%s %d %d %x\n", str_tmp.c_str()
			, strlen(words[0]), str_tmp.length(), words[0][5]);

	return 0;
}

