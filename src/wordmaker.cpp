/*
 * =====================================================================================
 *
 *       Filename:  wordmaker.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年02月17日 11时27分08秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Jannson, gandancing@gmail.com
 *
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
#include <string.h>
#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <thread>

#include <cedar.h>

using namespace std;

const uint32_t THREAD_N = 4;
const uint32_t LINE_LEN = 10240;
const uint32_t BUCKET_SIZE = 102400;
const uint32_t WORD_LEN = 6 * 2;//n个汉字
const uint32_t SHORTEST_WORD_LEN = 2;
const uint32_t LEAST_FREQ = 3;
const double   FREQ_LOG_MIN = -99999999.0;
static const size_t NUM_RESULT = 102400;

float g_entropy_thrhd = 1.7;

typedef list<string> string_list;
typedef shared_ptr<string_list> pstring_list;
typedef unordered_map<string, uint32_t> hash_t;
typedef shared_ptr<hash_t> phash_t;
typedef list<phash_t> phash_list;

typedef cedar::da<uint32_t> trie_t;
typedef shared_ptr<trie_t>	ptrie_t;
typedef list<ptrie_t>		ptrie_list;

class Log
{
public:
	Log(char* filename):fd(0)
	{
        fd = fopen(filename, "w");
	}
	void log(const char* fmt, ...)
	{
		va_list args;
		va_start(args, fmt);
		fprintf(fd, fmt, args);
		va_end(args);
	}
	~Log()
	{
		if(fd)
		{
			fclose(fd);
		}
	}
//private:
public:
	FILE* fd;
};

static Log glog("log.txt");

int gbk_char_len(const char* str)
{
    return (unsigned char)str[0] < 0x80 ? 1 : 2;
}

bool gbk_hanzi(const char* str)
{
    int char_len = gbk_char_len(str);
    if ((char_len == 2)
			&& ((unsigned char)str[0] < 0xa1 || (unsigned char)str[0] > 0xa9)
		) 
	{
        return true;
    }
    return false;
}

int unhanzi_to_space(char* oline, const char* iline)
{
    const uint32_t line_len = strlen(iline);
    uint32_t io = 0;
    bool is_prev_hanzi = false;
    for (uint32_t ii = 0; ii < line_len;) {
        const uint32_t char_len = gbk_char_len(iline + ii);
#ifdef DEBUG
        {
            string tmp(iline + ii, iline + ii + char_len);
            fprintf(stderr, "DEBUG, phrase[%s]\n", tmp.c_str());
        }
#endif
        if (gbk_hanzi(iline + ii)) {
            memcpy(oline + io, iline + ii, char_len);
            io += char_len;
            is_prev_hanzi = true;
#ifdef DEBUG
            string tmp(iline + ii, iline + ii + char_len);
            fprintf(stderr, "DEBUG, hanzi phrase[%s]\n", tmp.c_str());
#endif
        } else {
            if (is_prev_hanzi) {
                oline[io++] = ' ';
                is_prev_hanzi = false;
            }
        }
        ii += char_len;
        //BOOST_ASSERT(ii < line_len);
        if (ii > line_len) {
		//fprintf(stderr, "WARNING, last character[%s] length wrong line[%s]\n", iline + ii - char_len, iline);
		;/* Ignore the error print 30/10/13 11:22:11 */
        }
    }
    oline[io] = '\0';
    return 0;
}

struct Bucket
{
	pstring_list	hz_str;
	ptrie_t			trie;
};
typedef list<Bucket> BucketList;
typedef shared_ptr<BucketList> PBucketList;

class WordMaker;

void bucket_run(WordMaker& maker);

class WordMaker
{
public:
	WordMaker():word_num(0), step1_done(0), phz_str(make_shared<string_list>())
	{
		hzstr_list.push_back(phz_str);
	}

	int space_seperate_line_to_hanzi_vector(char* oline)
	{
		char* pch = strtok(oline, " ");
		while (pch != NULL) {
			string hz(pch);

			if(hz.length() > 2)
			{
				if(word_num >= BUCKET_SIZE)
				{
					//fprintf(stderr, "back size:%d hz size:%d\n", word_num, hzstr_list.size());

					phz_str = make_shared<string_list>();
					hzstr_list.push_back(phz_str);
					word_num = 0;
				}

				phz_str->push_back(hz);
				word_num++;
			}

			pch = strtok(NULL, " ");
		}
		return 0;
	}

	bool get_bucket(Bucket& bucket)
	{
		unique_lock<mutex> lock(m_var);

		if(hzstr_list.size() == 0)
		{
			return false;
		}

		pstring_list first = hzstr_list.front();
		hzstr_list.pop_front();
		bucket.hz_str = first;
		bucket.trie = make_shared<trie_t>();
		trie_list.push_back(bucket.trie);
		
		return true;
	}

	void run_step1()
	{
		thread threads[THREAD_N];
		for(int i = 0; i < THREAD_N; i++)
		{
			threads[i] = thread(bucket_run, ref(*this));
		}

		this->wait_all();
		for(int i = 0; i < THREAD_N; i++)
		{
			threads[i].join();
		}
	}

	void bucket_process(Bucket& bucket)
	{
		//std::this_thread::sleep_for(chrono::seconds(1));
		//fprintf(stderr, "size:%d\n", bucket.hz_str->size());
#if 0
		int len = 10;
		trie_t trie;
		
		string test_str;

		int i = 0;
		for(string_list::iterator it = bucket.hz_str->begin();
				it != bucket.hz_str->end(); it++)
		{
			if(i == 0)
			{
				test_str = string(it->begin(), it->begin()+2);
			}

			trie.update(it->c_str(), it->length(), i);
			i++;
			if( i > len)
			{
				break;
			}
		}

		//Test
		fprintf(glog.fd, "prefix: %s\n", test_str.c_str());

		char suffix[1024];
		typedef list<trie_t::result_triple_type> result_t;
		result_t result_triple;
		if(const size_t n = trie.commonPrefixPredict(test_str.c_str(), result_triple, NUM_RESULT))
		{
			fprintf(glog.fd, "n is %ld\n", n);
			for (result_t::iterator it = result_triple.begin(); it != result_triple.end(); it++)
			{
				trie.suffix(suffix, it->length, it->id);
				fprintf(glog.fd, "%d:%ld:%ld:%s%s\n", it->value
						, it->length, it->id, test_str.c_str(), suffix);
			}
		}
#endif

		for(string_list::iterator it = bucket.hz_str->begin();
				it != bucket.hz_str->end(); it++)
		{
			uint32_t phrase_len = min(static_cast<uint32_t>(it->length()), WORD_LEN);
			for (uint32_t i = 2; i <= phrase_len; i += 2)
			{
				for (string::iterator ic = it->begin(); ic < it->end() - i + 2; ic += 2)
				{
					string tmp(ic, ic + i);

					//add 1
					bucket.trie->update(tmp.c_str(), tmp.length(), 1);
				}
			}
		}
	}

	void nodify()
	{
		unique_lock<mutex> lock(m_var);
		step1_done++;
		fprintf(stderr, "done %d\n", step1_done);

		cond_var.notify_one();
	}

	void wait_all()
	{
		fprintf(stderr, "hear %d\n", __LINE__);
		unique_lock<mutex> lock(m_var);
		while(step1_done < THREAD_N)
		{
			cond_var.wait(lock);
		}
		fprintf(stderr, "all done\n");
	}

	void step1_reduce()
	{
		;
	}

private:
    list<pstring_list> hzstr_list;
	uint32_t word_num;
	pstring_list phz_str;

	ptrie_list		trie_list;
	trie_t			trie;
	trie_t			trie_reverse;
	
	int					step1_done;
	mutex				m_var;
	condition_variable	cond_var;
};

void bucket_run(WordMaker& maker)
{
	while(true)
	{
		Bucket bucket;
		if(maker.get_bucket(bucket))
		{
			maker.bucket_process(bucket);
		}
		else
		{
			break;
		}
	}

	//Notify main thread
	maker.nodify();
}

int main (int args, char* argv[])
{
    if (args != 3) {
        fprintf(stdout, "./wordmaker in_doc_file_name out_words_file_name\n");
        return -1;
    }
    const char* in_file_name = argv[1];
    const char* out_file_name = argv[2];
    FILE* fd_in = fopen(in_file_name, "r");
    char line[LINE_LEN];
    char oline[LINE_LEN];
	WordMaker wordmaker;

    while (fgets(line, LINE_LEN, fd_in)) {
        unhanzi_to_space(oline, line);
		wordmaker.space_seperate_line_to_hanzi_vector(oline);
    }
    fclose(fd_in);

	wordmaker.run_step1();

	return 0;
}
