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
const uint32_t	GBK_
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
			fclose(fd);
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
			&& ((unsiged char)str[0] < 0xa1 || (unsigned char)str[0] > 0xa9)
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
void reduce1(WordMaker& maker);
void gen_word_run(WordMaker& maker, int pos, int word_len);

class WordMaker
{
	struct sequence_trie_t: public trie_iter_t
	{
		sequence_trie_t(int ss, int ee, trie_t* trie1, trie_t* trie2):
			start(ss), end(ee), seq_trie(trie1), seq_trie_r(trie2)
		{
		}
		void operator()(trie_result_t& res) {
			char suffix[256];
			ptrie->suffix(suffix, res.length, res.id);
			if(((int)suffix[0] >= start) && ((int)suffix[0] < end)) {
				seq_trie.update(suffix, res.length, res.value);
			}

			string s(suffix);
			strrev_unicode(s);
			if(((int)suffix[0] >= start) && ((int)suffix[0] < end)) {
				seq_trie_r.update(s.c_str(), res.length, res.value);
			}
		}
		int			start;
		int			end;
		trie_t*		seq_trie;
		trie_t*		seq_trie_r;
	};
	struct cad_gen_t: public trie_iter_t
	{
		cad_gen_t(int p, uint32_t tol, trie_t* trie1, trie_t* trie2):
			pos(p), total_word(tol), seq_trie(trie1), seq_trie_r(trie2)
		{}
		float calc_entropy(const string& word
				, const trie_result_t& res
				, trie_t& entro_trie
				, const int total_freq) 
		{
			char suffix[256];
			hash_t		rlt_hash;
			trie_result_list rlts;
			size_t rlts_len = entro_trie.commonPrefixPredict(word.c_str(), rlts, NUM_RESULT);
			float entropy = 0.0;
			if(rlts_len > 1) {
				int entropy_freq = 0;
				// Ignore itself
				trie_result_list::iterator it = rlts.begin();
				for(it++; it != rlts.end(); it++) {
					assert(it->length < 250);
					entro_trie.suffix(suffix, it->length, it->id);
					string tmp_s(suffix);
					string tmp(tmp_s.begin(), tmp_s.begin()+2);
					//fprintf(glog.fd, "%s %s %s %d\n",word.c_str(), suffix, tmp.c_str(), it->value);

					hash_t::iterator it_map = rlt_hash.find(tmp);
					if (it_map == rlt_hash.end()) {
						rlt_hash[tmp] = it->value;
					} else {
						it_map->second += it->value;
					}

					entropy_freq += it->value;
				}
				for(hash_t::iterator map_it = rlt_hash.begin();
						map_it != rlt_hash.end();
						map_it++) {
					float p = static_cast<float>(map_it->second) / entropy_freq;
					entropy -= p * log(p);
					//fprintf(glog.fd, "entropy %s\t%d\n", map_it->first.c_str(), map_it->second);
				}
			} else {
				entropy = static_cast<float>(res.value)/20.0;
			}

			return entropy;
		}

		void operator()(trie_result_t& res)
		{
			char suffix[256];
			seq_trie.suffix(suffix, res.length, res.id);

			string word(suffix);
			uint32_t freq = res.value;

			int tmp_len = res.length;
			if (( tmp_len <= SHORTEST_WORD_LEN) || (tmp_len >= WORD_LEN)) {
				return;
			}
			if (freq < LEAST_FREQ) {
				return;
			}
			
			int total_freq = total_word/W;

			float max_ff = FREQ_LOG_MIN;
			for (string::iterator ic = word.begin() + 2; ic <= word.end() - 2; ic += 2) {
				string temp_left(word.begin(), ic);
				trie_result_t left_res;
				seq_trie.exactMatchSearch(left_res, temp_left.c_str());
				int left_f = left_res.value;
				if(left_f <= LEAST_FREQ)
					return;

				string temp_right(ic, word.end());
				trie_result_t right_res;
				seq_trie.exactMatchSearch(right_res, temp_right.c_str());
				int right_f = right_res.value;
				if(right_f <= LEAST_FREQ)
					return;

				float left = log(static_cast<float>(left_f)/total_freq);
				float right = log(static_cast<float>(right_f)/total_freq);
				max_ff = max(left + right, max_ff);
			}

			float log_freq = log(static_cast<float>(freq)/total_freq);
			log_freq -= max_ff;
			int weight = (word.size()-2)*0.5;
			log_freq -= 0.8*(weight-1);
			if(log_freq < 1.6) {
				return;
			}

			//Now calc right entropy
			float entropy_r = calc_entropy(word, res, pmaker->trie, total_freq);
			if(entropy_r < g_entropy_thrhd) {
				return;
			}

			// Now calc left entropy
			string word_r(word.c_str());
			strrev_unicode(word_r);
			assert(word_r.length() == word.length());

			//fprintf(glog.fd, "%s\t%s\t%f\n", word.c_str(), word_r.c_str(), log_freq);

			float entropy_l = calc_entropy(word_r, res, pmaker->trie_r, total_freq);
			if(entropy_l < g_entropy_thrhd) {
				return;
			}
			fprintf(pmaker->out_file.fd, "%s\t%f\t%f\t%f\n"
					, word.c_str(), log_freq, entropy_l, entropy_r);
		}
		int					pos;
		uint32_t			total_word;
		trie_t				seq_trie;
		trie_t				seq_trie_r;
		static const float	W = 4;
	};

public:
	WordMaker(const char* inf
			, const char* ouf,
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

		reduce_step1();
		run_step2();
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
		phz_str.clear();
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

		string trie_file(ofile_name + "_buck_" + to_string(bucket.id));
		bucket.trie.save(trie_file.c_str());
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

	int reduce1_next() {
		unique_lock<mutex> lock(m_var);
		return range_pos++;
	}

	void reduce_step1_run() {
		while(true) {
			int start, end;
			int pos = reduce1_next();
			gbk_range(start, end, pos, range_n);

			trie_t seq_trie, seq_trie_t;
			//Now only save the word from [start, end)
			for(int i = 0; i < total_bucket; i++) {
				string trie_file(ofile_name + "_buck_" + to_string(bucket.id));
				trie_t trie;
				trie.open(trie_file.c_str());

				sequence_trie_t seq_trie(start, end, this, &seq_trie, &seq_trie_r);
				trie.dump(seq_trie);
			}

			//string seq_file(ofile_name + "_seq_" + to_string(pos));
			//string seq_file_r(ofile_name + "_seqr_" + to_string(pos));
			//seq_trie.save(seq_file.c_str());
			//seq_trie_r.save(seq_file_r.c_str());

			//Now calc the result
			;
		}
	}

	void reduce_step1()
	{
		const int SEG = 2;

		//generate random tries to sequence tries
		range_pos = 0;
		range_n = (total_bucket + SEG - 1) / SEG;

		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(reduce1, ref(*this));
		}

		wait_threads_done();

		fprintf(stderr, "reduce ok with range:%d \n", range_n);
	}

	void gen_word(int pos, int word_len)
	{
		cad_gen_t cad_gen(this);
		trie.dump(cad_gen, pos, word_len);
	}

	void run_step2()
	{
		fprintf(stderr, "begin run_step2\n");

		int range = (total_word+thread_n-1) / thread_n;

		steps_done = 0;	//reset

		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(gen_word_run, ref(*this), i*range, range);
		}

		unique_lock<mutex> lock(m_var);
		while(steps_done < thread_n)
		{
			cond_var.wait(lock);
		}

		for(int i = 0; i < thread_n; i++) {
			threads[i].join();
		}

		fprintf(stderr, "step2 done\n");
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

void reduce1(WordMaker& maker) {
	maker.reduce_step1_run();
	maker.notify_step();
}

void gen_word_run(WordMaker& maker, int pos, int word_len)
{
	maker.gen_word(pos, word_len);
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
