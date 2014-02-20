/*
 * =====================================================================================
 *
 *       Filename:  wordmaker_big.cpp
 *
 *    Description:  For making more huge text
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

#include <marisa.h>

#define USE_FAST_LOAD
#include <cedar.h>

using namespace std;

const uint32_t	THREAD_N = 4;
const uint32_t	LINE_LEN = 10240;
const uint32_t	BUCKET_SIZE = 102400;
const uint32_t	WORKER_SIZE = 0x1000000; //16M
const uint32_t	WORD_LEN = 6 * 2;//n个汉字
const uint32_t	SHORTEST_WORD_LEN = 2;
const uint32_t	LEAST_FREQ = 3;
const float		FREQ_LOG_MIN = -99999999.0;
static const size_t NUM_RESULT = 102400;

const float g_entropy_thrhd = 1.7;

typedef list<string> string_list;
typedef shared_ptr<string_list> pstring_list;
typedef unordered_map<string, uint32_t> hash_t;
typedef shared_ptr<hash_t> phash_t;
typedef list<phash_t> phash_list;

typedef cedar::da<uint32_t> trie_t;
typedef shared_ptr<trie_t>	ptrie_t;
typedef list<ptrie_t>		ptrie_list;
typedef trie_t::result_triple_type trie_result_t;
typedef list<trie_result_t> trie_result_list;
typedef trie_t::iter_func trie_iter_t;

#if 0
/* Not work, why? */
typedef unique_ptr<std::FILE, int(*)(std::FILE*)> unique_file_ptr;
static auto make_file(const char* filename, const char* flags)
{
	FILE* f = fopen(filename, flags);
	return unique_file_ptr(f, fclose);
}
#endif

class WrapFile
{
	typedef FILE*	ptr;
	ptr				wrap_file;
public:
	WrapFile(const char* filename, const char* flag):
		wrap_file(fopen(filename, flag))
	{}
	operator ptr() const {
		return wrap_file;
	}
	~WrapFile()
	{
		if(wrap_file)
		{
			fclose(wrap_file);
		}
	}
};
//#ifdef DEBUG
static WrapFile glog("log.txt", "w");
//#endif

#define SWP(x,y) (x^=y, y^=x, x^=y)

void strrev_unicode(string& ref_s)
{
  int l = ref_s.length()/2;
  short *q, *p = (short*)&ref_s[0];
  q = p+l;
  for(--q; p < q; ++p, --q) SWP(*p, *q);
}

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

//get the range in [start, end)
void gbk_range(int& start, int& end, int pos, int split_n)
{
	//[81~A0] [B0~FE]
	const int total = (0xa0 - 0x80 + 1) + (0xfe - 0xb0 + 1);
	const int range = (total + split_n - 1)/split_n;

	//calc start
	start = 0x80 + pos * range;
	end = 0x80 + (pos + 1) * range;
	if((start <= 0xa0) && (end > 0xb0))
	{
		end += 0xb0 - 0xa0;
	}
	else if(start > 0xa0) {
		start += 0xb0 - 0xa0;
		end += 0xb0 - 0xa0;
	}
}

/* return the real iline len */
uint32_t unhanzi_to_space(char* oline, const char* iline)
{
    const uint32_t line_len = strlen(iline);
    uint32_t io = 0;
    bool is_prev_hanzi = false;
    for (uint32_t ii = 0; ii < line_len;) {
        const uint32_t char_len = gbk_char_len(iline + ii);
        if (gbk_hanzi(iline + ii)) {
            memcpy(oline + io, iline + ii, char_len);
            io += char_len;
            is_prev_hanzi = true;
        } else {
            if (is_prev_hanzi) {
                oline[io++] = ' ';
                is_prev_hanzi = false;
            }
        }
        ii += char_len;
        //assert(ii < line_len);
        if (ii > line_len) {
		//fprintf(glog, "WARNING, last character[%s] length wrong line[%s]\n", iline + ii - char_len, iline);
		;/* Ignore the error print 30/10/13 11:22:11 */
        }
    }
    oline[io] = '\0';
    return line_len;
}

struct Bucket
{
	int				id;
	pstring_list	hz_str;
	ptrie_t			trie;
};
typedef list<Bucket> BucketList;
typedef shared_ptr<BucketList> PBucketList;

class WordMaker;

void bucket_run(WordMaker& maker);

class WordMaker
{
	struct trie_combine_t:public trie_iter_t
	{
		trie_combine_t(trie_t* trie, marisa::Keyset* kset): keyset(kset), ptrie(trie)
		{
		}
		void operator()(trie_result_t& res)
		{
			char suffix[256];
			ptrie->suffix(suffix, res.length, res.id);
			string s(suffix);
			keyset->push_back(s.c_str(), s.length(), (float)res.value);
		}
		trie_t* ptrie;
		marisa::Keyset* keyset;
	};
public:
	WordMaker(const char* inf
			, const char* ouf
			, int thr=THREAD_N):bucket_num(0)
							, total_bucket(0)
							, total_word(0)
							, steps_done(0)
							, thread_n(thr)
							, phz_str(make_shared<string_list>())
							, threads(new thread[thr])
							, in_file(inf, "r")
							, ofile_name(ouf)
	{
	}

	/* Only run in the main thread */
	bool bulk_text()
	{
		char line[LINE_LEN];
		char oline[LINE_LEN];
		uint32_t bulk_len = 0;

		while (fgets(line, LINE_LEN, in_file)) {
			uint32_t l = unhanzi_to_space(oline, line);
			space_seperate_line_to_hanzi_vector(oline);

			bulk_len += l;
			if(bulk_len >= WORKER_SIZE){
				return true;
			}
		}
		
		/* have to process the last */
		return false;
	}

	void workers_run()
	{
		int workers = 0;
		bool next = true;

		while(next) {
			reset_step1();
			next = bulk_text();
			run_step1();

			workers++;
			fprintf(stderr, "workers %d done, total_words:%d\n", workers, total_word);
		}
		fprintf(stderr, "step1 done\n");

		marisa::Keyset kset;
		for(int i = 0; i < total_bucket; i++) {
			trie_t trie;
			string trie_file(ofile_name + "_buck_" + std::to_string((long long int)i));
			trie.open(trie_file.c_str());
			trie_combine_t combine(&trie, &kset);
			trie.dump(combine);
		}
		marisa::Trie mar_trie;
		mar_trie.build(kset, MARISA_LABEL_ORDER);
		string mar_file(ofile_name + "_mar_");
		mar_trie.save(mar_file.c_str());
		fprintf(stderr, "marsa trie saved\n");
	}

	int space_seperate_line_to_hanzi_vector(char* oline)
	{
		char* pch = strtok(oline, " ");
		while (pch != NULL) {
			string hz(pch);

			if(hz.length() > 2)
			{
				if(bucket_num >= BUCKET_SIZE)
				{
					//fprintf(stderr, "back size:%d hz size:%d\n", bucket_num, hzstr_list.size());

					phz_str = make_shared<string_list>();
					hzstr_list.push_back(phz_str);
					bucket_num = 0;
				}

				phz_str->push_back(hz);
				bucket_num++;
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
		bucket.id = total_bucket++;
		
		return true;
	}

	void reset_step1() {
		assert(hzstr_list.size() == 0);
		
		bucket_num = 0;
		steps_done = 0;
		phz_str->clear();
		hzstr_list.push_back(phz_str);
	}

	void run_step1()
	{
		fprintf(stderr, "run_step1 using thread:%d\n", thread_n);

		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(bucket_run, ref(*this));
		}

		wait_threads_done();
	}

	void bucket_process(Bucket& bucket)
	{
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

		string trie_file(ofile_name + "_buck_" + std::to_string((long long int)bucket.id));
		bucket.trie->save(trie_file.c_str());
		fprintf(stderr, "bucket %d done\n", bucket.id);
	}

	void notify_step()
	{
		unique_lock<mutex> lock(m_var);
		//fprintf(stderr, "done %d\n", steps_done);
		steps_done++;
		cond_var.notify_one();
	}

	void wait_threads_done()
	{
		unique_lock<mutex> lock(m_var);
		while(steps_done < thread_n)
		{
			cond_var.wait(lock);
		}

		for(int i = 0; i < thread_n; i++) {
			threads[i].join();
		}
	}

private:
    list<pstring_list> hzstr_list;
	uint32_t		bucket_num;
	pstring_list	phz_str;
	uint32_t		total_bucket;

	//ptrie_list		trie_list;
	//trie_t			trie;
	//trie_t			trie_r;
	uint32_t		total_word;

	int				range_pos;
	int				range_n;
	
	int				thread_n;
	unique_ptr<thread[]> threads;
	int				steps_done;
	mutex			m_var;
	condition_variable	cond_var;

	string			ofile_name;
	WrapFile		in_file;
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
	
	maker.notify_step();
}

int main (int args, char* argv[])
{
    if (args != 3) {
        fprintf(stdout, "./wordmaker in_doc_file_name out_words_file_name\n");
        return -1;
    }

	WordMaker wordmaker(argv[1], argv[2]);
	wordmaker.workers_run();

	return 0;
}
