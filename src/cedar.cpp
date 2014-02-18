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
#include <cstdio>
#include <cstdlib>
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
	void operator()(result_t& res)
	{
		fprintf(stderr, "ResOper %d\n", __LINE__);
	}
	int a;
};
struct ResOper2:public iter_func_t
{
	void operator()(result_t& res)
	{
		fprintf(stderr, "ResOper2 %d\n", __LINE__);
	}
	int b;
};

void test_res(result_t& res)
{
	fprintf(stderr, "ResOper %d\n", __LINE__);
}

int main(int argc, char** argv)
{
	char* words[] = {"apple", "at", "app", "aword", "awt", "我们", "我"};
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

#if 0
	if(const size_t n = trie.commonPrefixPredict(word, result_triple, NUM_RESULT))
	{
		fprintf(stderr, "n is %ld\n", n);
		for (result_t::iterator it = result_triple.begin(); it != result_triple.end(); it++)
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

	fprintf(stderr, "begin dump %d\n", __LINE__);
	ResOper2 res;
	//std::function<void(result_t&)> res = test_res;
	trie.dump(res);

	return 0;
}

