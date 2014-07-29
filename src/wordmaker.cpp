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
const float   FREQ_LOG_MIN = -99999999.0;
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
typedef unique_ptr<FILE, int (*)(FILE*)> unique_file_ptr;

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

#ifdef DEBUG
static WrapFile glog("log.txt", "w");
#endif

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
	unsigned char cmps[3][4] = {{0xb0,0xf7,0xa1,0xfe}
							, {0x81,0xa0,0x40,0xfe}
							, {0xaa,0xfe,0x40,0xa0} };

    int char_len = gbk_char_len(str);
    if (char_len == 2)
			/*&& ( ((unsigned char)str[0] < 0xa1 || (unsigned char)str[0] > 0xa9)*/
	{
		for(int i = 0; i < 3; i++) {
			if( ((unsigned char)str[0] >= cmps[i][0])
					&& ((unsigned char)str[0] <= cmps[i][1])
					&& ((unsigned char)str[1] >= cmps[i][2])
					&& ((unsigned char)str[1] <= cmps[i][3])
			  ) {
				return true;
			}
		}
        return false;
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
	int				id;
	pstring_list	hz_str;
	ptrie_t			trie;
};
typedef list<Bucket> BucketList;
typedef shared_ptr<BucketList> PBucketList;

class WordMaker;

void bucket_run(WordMaker& maker);
void gen_word_run(WordMaker& maker, int pos, int word_len);

class WordMaker
{
	struct trie_combine_t:public trie_iter_t
	{
		trie_combine_t(ptrie_t pt, WordMaker* maker):ptrie(pt)
											, pmaker(maker)
											, word_len(0)
		{}
		void operator()(trie_result_t& res)
		{
			char suffix[256];
			ptrie->suffix(suffix, res.length, res.id);

			pmaker->trie.update(suffix, res.length, res.value);
			
			string s(suffix);
			strrev_unicode(s);
			pmaker->trie_r.update(s.c_str(), res.length, res.value);
			//fprintf(glog.fd, "%s\t:\t%s\n", suffix, s.c_str());

			word_len++;
		}
		size_t word_len;
		ptrie_t ptrie;
		WordMaker* pmaker;
	};

	struct cad_gen_t:public trie_iter_t
	{
		cad_gen_t(WordMaker* maker):pmaker(maker)
		{}
		float calc_entropy(const string& word
				, const trie_result_t& res
				, trie_t& entro_trie
				, const uint32_t total_freq) 
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
			static int gtest = 1000;

			char suffix[256];
			pmaker->trie.suffix(suffix, res.length, res.id);
			//fprintf(glog.fd, "word:%s\t%d\n", suffix, res.id);

			string word(suffix);
			uint32_t freq = res.value;

			//fprintf(glog.fd, "word:%s\t%d\n", word.c_str(), freq);

			int tmp_len = res.length;
			if (( tmp_len <= SHORTEST_WORD_LEN) || (tmp_len >= WORD_LEN)) {
				return;
			}
			if (freq < LEAST_FREQ) {
				return;
			}
			
			uint32_t total_freq = pmaker->total_word/W;
			if(gtest == 1000){
				fprintf(stderr, "total_word:%d\n", pmaker->total_word);
				gtest--;
			}

			float max_ff = FREQ_LOG_MIN;
			for (string::iterator ic = word.begin() + 2; ic <= word.end() - 2; ic += 2) {
				string temp_left(word.begin(), ic);
				trie_result_t left_res;
				pmaker->trie.exactMatchSearch(left_res, temp_left.c_str());
				int left_f = left_res.value;
				if(left_f <= LEAST_FREQ)
					continue;

				string temp_right(ic, word.end());
				trie_result_t right_res;
				pmaker->trie.exactMatchSearch(right_res, temp_right.c_str());
				int right_f = right_res.value;
				if(right_f <= LEAST_FREQ)
					continue;

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
			//fprintf(pmaker->out_file, "%s\t%f\t%f\t%f\n"
			//		, word.c_str(), log_freq, entropy_l, entropy_r);
			fprintf(pmaker->out_file, "%s\t%d\n", word.c_str(), res.value);
		}
		WordMaker* pmaker;
		const float W = 4;
	};

public:
	WordMaker(const char* filename
			, int thr=THREAD_N):bucket_num(0)
							, total_bucket(0)
							, total_word(0)
							, step1_done(0)
							, thread_n(thr)
							, phz_str(make_shared<string_list>())
							//, threads(new thread[thr])
							, out_file(filename, "w")
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

	void run_step1()
	{
		fprintf(stderr, "begin run_step1 using thread:%d\n", thread_n);

		unique_ptr<thread[]> nths(new thread[thread_n]);
		threads = std::move(nths);
		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(bucket_run, ref(*this));
		}

		this->reduce_step1();

		//trie.save("test.trie");
		//trie_r.save("test2.trie");
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

		// Notify the main thread to do the job
		unique_lock<mutex> lock(m_var);
		trie_list.push_back(bucket.trie);
		cond_var.notify_one();
		fprintf(stderr, "bucket %d done\n", bucket.id);
	}

	void notify_step1()
	{
		unique_lock<mutex> lock(m_var);
		fprintf(stderr, "done %d\n", step1_done);
		step1_done++;
		cond_var.notify_one();
	}

	//Only run in main thread
	void reduce_step1()
	{
		//Combile all tries to one trie

		while(true)
		{
			// TODO how to do better for this
			unique_lock<mutex> lock(m_var);
			while((step1_done < thread_n) && trie_list.empty())
			{
				cond_var.wait_for(lock, std::chrono::milliseconds(1000*30));
			}
			if(step1_done < thread_n)
			{
				//fprintf(stderr, "begin combine %d\n", __LINE__);
				ptrie_t ptrie = trie_list.front();
				trie_combine_t trie_combine(ptrie, this);
				ptrie->dump(trie_combine);
				trie_list.pop_front();
				//fprintf(stderr, "end combine %d\n", __LINE__);

				total_word += trie_combine.word_len;
			}
			else
				break;
		}

		// Check that all thread exit
		for(int i = 0; i < thread_n; i++)
		{
			assert(threads[i].joinable());
			try {
				threads[i].join();
			}
			catch(std::system_error& s_e) {
				fprintf(stderr, "sys error in thread:%d\n", i);
			}
		}

		fprintf(stderr, "thread all done\n");

		unique_lock<mutex> lock(m_var);
		for(ptrie_list::iterator it = trie_list.begin();
				it != trie_list.end(); it++)
		{
			ptrie_t ptrie = *it;
			trie_combine_t trie_combine(ptrie, this);
			ptrie->dump(trie_combine);

			total_word += trie_combine.word_len;
		}

		fprintf(stderr, "reduce %d words ok\n", total_word);
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

		step1_done = 0;	//reset

		unique_ptr<thread[]> nths(new thread[thread_n]);
		threads = std::move(nths);
		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(gen_word_run, ref(*this), i*range, range);
		}

		if(step1_done < thread_n)
		{
			unique_lock<mutex> lock(m_var);
			while(step1_done < thread_n)
			{
				cond_var.wait(lock);
			}
		}

		for(int i = 0; i < thread_n; i++) {
			assert(threads[i].joinable());
			try {
				threads[i].join();
			}
			catch(std::system_error& s_e) {
				fprintf(stderr, "sys error in thread:%d\n", i);
			}
		}

		fprintf(stderr, "step2 done\n");
	}

private:
    list<pstring_list> hzstr_list;
	uint32_t		bucket_num;
	uint32_t		total_bucket;
	pstring_list	phz_str;

	ptrie_list		trie_list;
	trie_t			trie;
	trie_t			trie_r;
	uint32_t		total_word;
	
	int				thread_n;
	unique_ptr<thread[]> threads;
	volatile int	step1_done;
	mutex			m_var;
	condition_variable	cond_var;

	WrapFile		out_file;
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
	
	maker.notify_step1();
}

void gen_word_run(WordMaker& maker, int pos, int word_len)
{
	maker.gen_word(pos, word_len);

	maker.notify_step1();	/* Use the same notify */
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
	WordMaker wordmaker(out_file_name);

    while (fgets(line, LINE_LEN, fd_in)) {
        unhanzi_to_space(oline, line);
		wordmaker.space_seperate_line_to_hanzi_vector(oline);
    }
    fclose(fd_in);

	wordmaker.run_step1();

	wordmaker.run_step2();

	return 0;
}
