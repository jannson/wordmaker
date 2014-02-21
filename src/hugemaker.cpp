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

#define USE_EXACT_FIT
#define USE_FAST_LOAD
#include <cedar.h>

using namespace std;

const uint32_t	THREAD_N = 4;
const uint32_t	LINE_LEN = 10240;
const uint32_t	BUCKET_SIZE = 2*1024*1024;
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

string _to_string(int nu) {
	char s[250];
	sprintf(s, "%d", nu);
	return string(s);
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

//get the range in [start, end)
void gbk_range(int& start, int& end, int pos, int split_n)
{
	if (split_n > 3) {
		const int RANGE_START = 0xb4;
		const int RANGE_END = 0xf1;
		if (0 == pos) {
			start = 0x80;
			end = RANGE_START;
		}
		else if(pos == (split_n - 1)){
			start = RANGE_END;
			end = 0xff;
		}
		else {
			int n = split_n - 2;
			const int total = (RANGE_END - RANGE_START);
			const int range = (total + n - 1)/n;
			start = RANGE_START + (pos - 1)*range;
			end = RANGE_START + pos * range;
			if (end >= RANGE_END) {
				end = RANGE_END;
			}
		}
	}
	else {
		//[81~A0] [B0~FE]
		const int total = (0xa0 - 0x80 + 1) + (0xfe - 0xb0 + 1);
		const int range = (total + split_n - 1)/split_n;

		//calc start
		start = 0x80 + pos * range;
		end = 0x80 + (pos + 1) * range;
		if((start <= 0xa0) && (end > 0xa0))
		{
			end += 0xb0 - 0xa0;
		}
		else if(start > 0xa0) {
			start += 0xb0 - 0xa0;
			end += 0xb0 - 0xa0;
		}
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

struct cad_word_result
{
	string		word;
	int			freq;
};
typedef list<cad_word_result> cad_result_list;

class WordMaker;

void bucket_run(WordMaker& maker);
void reduce1(WordMaker& maker);
void map_step2(WordMaker& maker, cad_result_list& cad_results);

bool cad_word_weight_cmp(const cad_word_result & w1, const cad_word_result& w2) {
	return (w1.freq > w2.freq);
}

class WordMaker
{
	struct sequence_combine_t: public trie_iter_t
	{
		sequence_combine_t(uint8_t ss, uint8_t ee
				, trie_t* trie_this, trie_t* trie2_seq):
				start(ss), end(ee), this_trie(trie_this), seq_trie(trie2_seq)
		{
			//fprintf(stderr, "seq combine 0x%02x 0x%02x\n", ss, ee);
		}
		void operator()(trie_result_t& res) {
			char suffix[256];
			this_trie->suffix(suffix, res.length, res.id);
			
			if(((uint8_t)suffix[0] >= start) && ((uint8_t)suffix[0] < end)) {
				//fprintf(stderr, "hear happen %02x %d\n", (int)suffix[0], __LINE__);
				seq_trie->update(suffix, res.length, res.value);
			}
		}
		uint8_t			start;	//Fix bugs for compare with int
		uint8_t			end;
		trie_t*		this_trie;
		trie_t*		seq_trie;
	};
	struct reverse_combine_t:public trie_iter_t
	{
		reverse_combine_t(trie_t* trie, marisa::Keyset* kset, vector<uint32_t>* ws)
			: keyset(kset), ptrie(trie), weights(ws)
		{
		}
		void operator()(trie_result_t& res)
		{
			char suffix[256];
			ptrie->suffix(suffix, res.length, res.id);
			string s(suffix);
			strrev_unicode(s);
			keyset->push_back(s.c_str(), s.length());
			weights->push_back(res.value);
		}
		marisa::Keyset* keyset;
		trie_t* ptrie;
		vector<uint32_t>* weights;
	};
	struct check_weight_t : public trie_iter_t
	{
		check_weight_t(trie_t* trie1, marisa::Trie* trie2, vector<uint32_t>* ws)
			: ptrie(trie1), martrie(trie2), weights(ws)
		{
		}
		void operator()(trie_result_t& res)
		{
			char suffix[256];
			ptrie->suffix(suffix, res.length, res.id);
			string s(suffix);
			strrev_unicode(s);
	
			marisa::Agent agent;
			agent.set_query(s.c_str());
			if(martrie->lookup(agent)) {
				assert(res.value == (*weights)[agent.key().id()]);
			}
			else {
				assert(false);
			}
		}
		trie_t*				ptrie;
		marisa::Trie*		martrie;
		vector<uint32_t>*	weights;
	};
	struct cad_gen_t: public trie_iter_t
	{
		cad_gen_t(int p, unsigned char ss, unsigned char ee
				, uint32_t to_word, trie_t* trie1
				, marisa::Trie* trie2, vector<uint32_t>* ws,  cad_result_list* pres):
			pos(p), start(ss), end(ee), total_word(to_word)
			, seq_trie(trie1), mar_trie(trie2), weights(ws), presults(pres)
		{
		}
		float calc_entropy(const string& word
				, const trie_result_t& res
				, const uint32_t total_freq) 
		{
			char suffix[256];
			hash_t		rlt_hash;
			trie_result_list rlts;
			size_t rlts_len = seq_trie->commonPrefixPredict(word.c_str(), rlts, NUM_RESULT);
			float entropy = 0.0;
			if(rlts_len > 1) {
				int entropy_freq = 0;
				// Ignore itself
				trie_result_list::iterator it = rlts.begin();
				for(it++; it != rlts.end(); it++) {
					assert(it->length < 250);
					seq_trie->suffix(suffix, it->length, it->id);
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
				}
			} else {
				entropy = static_cast<float>(res.value)/20.0;
			}

			return entropy;
		}

		float calc_entropy_marisa(const string& word
				, const trie_result_t& res
				, const uint32_t total_freq) 
		{
			hash_t		rlt_hash;
			float entropy = 0.0;
			marisa::Agent agent;
			agent.set_query(word.c_str());
			int word_l = word.length();

			if(mar_trie->predictive_search(agent)) {
				int entropy_freq = 0;
				while(mar_trie->predictive_search(agent)) {
					string tmp(agent.key().ptr() + word_l, agent.key().ptr() + word_l + 2);
					hash_t::iterator it_map = rlt_hash.find(tmp);
					if (it_map == rlt_hash.end()) {
						rlt_hash[tmp] = (*weights)[agent.key().id()];
					} else {
						it_map->second += (*weights)[agent.key().id()];
					}
					entropy_freq += (*weights)[agent.key().id()];
				}
				for(hash_t::iterator map_it = rlt_hash.begin();
						map_it != rlt_hash.end();
						map_it++) {
					float p = static_cast<float>(map_it->second) / entropy_freq;
					entropy -= p * log(p);
				}
			} else {
				entropy = static_cast<float>(res.value)/20.0;
			}

			return entropy;
		}

		void operator()(trie_result_t& res)
		{
			char suffix[256];
			seq_trie->suffix(suffix, res.length, res.id);

			string word(suffix);
			uint32_t freq = res.value;

			int tmp_len = res.length;
			if (( tmp_len <= SHORTEST_WORD_LEN) || (tmp_len >= WORD_LEN)) {
				return;
			}
			if (freq < LEAST_FREQ) {
				return;
			}
			
			uint32_t total_freq = total_word/W;

			float max_ff = FREQ_LOG_MIN;
			for (string::iterator ic = word.begin() + 2; ic <= word.end() - 2; ic += 2) {
				string temp_left(word.begin(), ic);
				trie_result_t left_res;
				//The left word is already in this trie
				assert(((uint8_t)temp_left[0] >= start) && ((uint8_t)temp_left[0] < end));
				seq_trie->exactMatchSearch(left_res, temp_left.c_str());
				int left_f = left_res.value;
				if(left_f <= LEAST_FREQ)
					continue;
				
				string temp_right(ic, word.end());
				trie_result_t right_res;
				int right_f = 0;
				if(((uint8_t)temp_right[0] >= start) && ((uint8_t)temp_right[0] < end)) {
					//The right word is in this trie too
					seq_trie->exactMatchSearch(right_res, temp_right.c_str());
					right_f = right_res.value;
					//fprintf(glog, "%s\t%s freq:%d\n", word.c_str(), temp_right.c_str(), right_res.value); 
				} else {
					//Search from marisa trie
					marisa::Agent agent;
					string rev_s(temp_right.c_str());
					strrev_unicode(rev_s);
					agent.set_query(rev_s.c_str());
					if(mar_trie->lookup(agent)) {
						right_f = (*weights)[agent.key().id()];
					}
				}
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
			float entropy_r = calc_entropy(word, res, total_freq);
			if(entropy_r < g_entropy_thrhd) {
				return;
			}

			// Now calc left entropy
			string word_r(word.c_str());
			strrev_unicode(word_r);
			//assert(word_r.length() == word.length());

			float entropy_l = calc_entropy_marisa(word_r, res, total_freq);
			if(entropy_l < g_entropy_thrhd) {
				return;
			}
			cad_word_result cad_word;
			cad_word.word = word;
			cad_word.freq = res.value;
			presults->push_back(cad_word);
			//fprintf(pmaker->out_file.fd, "%s\t%f\t%f\t%f\n"
			//		, word.c_str(), log_freq, entropy_l, entropy_r);
		}
		uint8_t				pos;
		uint8_t				start;
		int					end;
		uint32_t			total_word;
		trie_t*				seq_trie;
		marisa::Trie*		mar_trie;
		vector<uint32_t>*	weights;
		cad_result_list*	presults;
		static const uint32_t	W = 1;
	};
public:
	WordMaker(const char* inf
			, const char* ouf
			, int thr=THREAD_N):bucket_num(0)
							, phz_str(make_shared<string_list>())
							, total_bucket(0)
							, total_word(0)
							, steps_done(0)
							, thread_n(thr)
							//, threads(new thread[thr]) Init it after
							, in_file(inf, "r")
							, ofile_name(ouf)
	{
	}
	~WordMaker() {
		remove_buck_files();
		remove_seq_files();
	}

	void remove_buck_files() {
		for(int i = 0; i < total_bucket; i++) {
			string trie_file(ofile_name + "_buck_" + _to_string(i));
			std::remove(trie_file.c_str());
			string trie_file2(ofile_name + "_buck_" + _to_string(i)+".sbl");
			std::remove(trie_file2.c_str());
		}
	}

	void remove_seq_files() {
		for(int i = 0; i < seq_range_n; i++) {
			string trie_file(ofile_name + "_seq_" + _to_string(i));
			std::remove(trie_file.c_str());
			string trie_file2(ofile_name + "_seq_" + _to_string(i)+".sbl");
			std::remove(trie_file2.c_str());
		}
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

		if(!in_file) {
			fprintf(stderr, "Cannot open the input file\n");
		}

		while(next) {
			reset_step1();
			next = bulk_text();
			run_step1();

			workers++;
			fprintf(stderr, "workers %d done, total_words:%d next:%d \n", workers, total_word, next);
		}
		fprintf(stderr, "step1 done\n");

		reduce_step1();
		fprintf(stderr, "step2 done\n");

		gen_cad_words();
	}

	int space_seperate_line_to_hanzi_vector(char* oline)
	{
		char* pch = strtok(oline, " ");
		while (pch != NULL) {
			string hz(pch);
			size_t hz_l = hz.length();
			assert(hz_l < 1024*2);
			if(hz_l > 2)
			{
				if(bucket_num >= BUCKET_SIZE)
				{
					//fprintf(stderr, "back size:%d hz size:%d\n", bucket_num, hzstr_list.size());

					phz_str = make_shared<string_list>();
					hzstr_list.push_back(phz_str);
					bucket_num = 0;
				}

				phz_str->push_back(hz);
				bucket_num += hz_l;
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
		//assert(first->size() != 0);
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
		fprintf(stderr, "run_step1 using threads:%d\n", thread_n);

		unique_ptr<thread[]> nths(new thread[thread_n]);
		threads = std::move(nths);

		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(bucket_run, ref(*this));
		}

		wait_threads_done();
	}

	void bucket_process(Bucket& bucket)
	{
		const uint32_t loop_ever = 5123456;

		uint32_t word_l = 0;
		for(string_list::iterator it = bucket.hz_str->begin();
				it != bucket.hz_str->end(); it++)
		{
			//TODO for WORD_LEN-2
			size_t it_len = it->length();
			uint32_t phrase_len = min(static_cast<uint32_t>(it_len), WORD_LEN);
			assert(it_len < 1024*2);
			for (uint32_t i = 2; i <= phrase_len; i += 2)
			{
				for (string::iterator ic = it->begin(); ic < it->end() - i + 2; ic += 2)
				{
					string tmp(ic, ic + i);
					//add 1
					bucket.trie->update(tmp.c_str(), i, 1);
					word_l++;

					assert(word_l < loop_ever);
				}

			}
		}

		if(word_l > 0) {

			string trie_file(ofile_name + "_buck_" + _to_string(bucket.id));
			bucket.trie->save(trie_file.c_str());
			fprintf(stderr, "bucket %d done with words:%d\n", bucket.id, word_l);

			unique_lock<mutex> lock(m_var);
			total_word += word_l;
		}
	}

	void notify_step()
	{
		unique_lock<mutex> lock(m_var);
		//fprintf(stderr, "done %d\n", steps_done);
		steps_done++;
		//cond_var.notify_one();
	}

	void wait_threads_done()
	{
		// Fix dead lock bug hear 20/02/14 16:27:03
		// How to do better for this?
#if 0
		if(steps_done < thread_n) {	
			unique_lock<mutex> lock(m_var);
			while(steps_done < thread_n)
			{
				cond_var.wait_for(lock, std::chrono::milliseconds(1000*30));
			}
		}
#endif

#if 1

		for(int i = 0; i < thread_n; i++) {
			assert(threads[i].joinable());
			try {
				threads[i].join();
			}
			catch(std::system_error& s_e) {
				fprintf(stderr, "sys error in thread:%d\n", i);
			}
		}
#endif

	}

	int reduce1_next() {
		unique_lock<mutex> lock(m_var);
		return seq_range_pos++;
	}

	void reduce_step1_run() {
		const int max_start = 0xfe;

		while(true) {
			int start, end;
			int pos = reduce1_next();
			//fprintf(stderr, "begin reduce:%d \n", pos);

			if(pos >= seq_range_n) {
				break;
			}
			gbk_range(start, end, pos, seq_range_n);
			if(start > max_start) {
				break;
			}
			//fprintf(stderr, "get pos:%d start:0x%02x end:0x%02x\n", pos, start, end);

			trie_t seq_trie;
			//Now only save the word from [start, end)
			for(int i = 0; i < total_bucket; i++) {
				string trie_file(ofile_name + "_buck_" + _to_string(i));
				trie_t trie;

				if(-1 == trie.open(trie_file.c_str())) {
					fprintf(stderr, "open %s trie error\n", trie_file.c_str());
					continue;
				}
				//Ignore the ok print
				//else {
				//	fprintf(stderr, "open %s trie ok keys:%d\n", trie_file.c_str(), trie.num_keys());
				//}

				sequence_combine_t seq_combine((uint8_t)start, (uint8_t)end, &trie, &seq_trie);
				trie.dump(seq_combine);
			}
			
			string seq_file(ofile_name + "_seq_" + _to_string(pos));
			seq_trie.save(seq_file.c_str());
			fprintf(stderr, "pos:%d done saved keys:%d\n", pos, seq_trie.num_keys());
		}
	}
	
	void reduce_step1()
	{
		const int SEG = 2;

		//generate random tries to sequence tries
		seq_range_pos = 0;
		seq_range_n = (total_bucket + SEG - 1) / SEG + 2;
		if(seq_range_n < thread_n) {
			seq_range_n = thread_n;
		}

		fprintf(stderr, "reduce with range:%d thread:%d \n", seq_range_n, thread_n);
		unique_ptr<thread[]> nths(new thread[thread_n]);
		threads = std::move(nths);

		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(reduce1, ref(*this));
		}

		wait_threads_done();

		fprintf(stderr, "reduce ok total buck:%d with range:%d \n", total_bucket, seq_range_n);
		//remove_buck_files();

		marisa::Keyset kset;
		vector<uint32_t> weights;

		for(int i = 0; i < seq_range_n; i++) {
			trie_t trie;
			string trie_file(ofile_name + "_seq_" + _to_string(i));
			int open_status = trie.open(trie_file.c_str());
			if(-1 != open_status) {
				fprintf(stderr, "open %s ok keys:%d\n", trie_file.c_str(), trie.num_keys());
			}
			assert(-1 != open_status);
			reverse_combine_t combine(&trie, &kset, &weights);
			trie.dump(combine);
		}
		assert(weights.size() == kset.size());
		fprintf(stderr, "build marisa trie. real total words:%d \n", kset.size());
		total_word = kset.size();
		//Use 9 tries for space-efficient
		mar_trie.build(kset, MARISA_LABEL_ORDER|MARISA_HUGE_CACHE|9);

		/* If the file is so large
		 * , You can split the mar_trie into many files and use mmap api of
		 * mar_trie
		 */
		//string mar_file(ofile_name + "_mar_");
		//mar_trie.save(mar_file.c_str());
		//fprintf(stderr, "marsa trie saved\n");
		
		//weights_all.reserve(kset.size());
		weights_all = weights;
		for(int i = 0; i < kset.size(); i++) {
			weights_all[kset[i].id()] = weights[i];
		}

		/* Just test for weights */
#if 0
		fprintf(stderr, "Checking the weights\n");
		for(int i = 0; i < seq_range_n; i++) {
			trie_t trie;
			string trie_file(ofile_name + "_seq_" + _to_string(i));
			int open_status = trie.open(trie_file.c_str());
			if(-1 != open_status) {
				fprintf(stderr, "open %s ok keys:%d\n", trie_file.c_str(), trie.num_keys());
			}
			assert(-1 != open_status);
			check_weight_t check_weight(&trie, &mar_trie, &weights_all);
			trie.dump(check_weight);
		}
#endif

	}

	int seq_range_next() {
		unique_lock<mutex> lock(m_var);
		return seq_range_pos++;
	}

	void map_step2_run(cad_result_list& cad_results) {
		const int max_start = 0xfe;

		while(true) {
			int start, end;
			int pos = seq_range_next();
			if(pos >= seq_range_n) {
				break;
			}
			gbk_range(start, end, pos, seq_range_n);
			if(start > max_start) {
				break;
			}
			trie_t trie;
			string trie_file(ofile_name + "_seq_" + _to_string(pos));
			trie.open(trie_file.c_str());

			cad_gen_t cad_gen(pos, (uint8_t)start, (uint8_t)end, total_word, &trie
					, &mar_trie, &weights_all, &cad_results);
			trie.dump(cad_gen);
		}
	}

	void gen_cad_words() {
		/* reset the range pos */
		seq_range_pos = 0;
	
		fprintf(stderr, "generating words\n");
		/* Create sub tries for pos */
		auto result_lists = unique_ptr<cad_result_list[]>(new cad_result_list[seq_range_n]);

		unique_ptr<thread[]> nths(new thread[thread_n]);
		threads = std::move(nths);
		for(int i = 0; i < thread_n; i++)
		{
			threads[i] = thread(map_step2, ref(*this), ref(result_lists[i]));
		}

		wait_threads_done();

		fprintf(stderr, "generate valid words ok\n");
		reduce2(result_lists);
	}

	void reduce2(unique_ptr<cad_result_list[]>& result_lists) {
		//Change to false to order by word
		bool sort_weight = true;
		
		WrapFile ofile(ofile_name.c_str(), "w");
		if(sort_weight){
			fprintf(stderr, "Sorted the results by weight\n");
			cad_result_list weight_results;
			for(int i = 0; i < seq_range_n; i++) {
				cad_result_list& result_list = result_lists[i];
				result_list.sort(cad_word_weight_cmp);
				weight_results.merge(result_list, cad_word_weight_cmp);
			}
			for(cad_result_list::iterator it = weight_results.begin();
					it != weight_results.end(); it++) {
				fprintf(ofile, "%s\t%d\n", it->word.c_str(), it->freq);
			}
		} else {
			/* Sorted by word already */
			fprintf(stderr, "Sorted the results by word\n");
			for(int i = 0; i < seq_range_n; i++) {
				cad_result_list& result_list = result_lists[i];
				for(cad_result_list::iterator it = result_list.begin();
						it != result_list.end(); it++) {
					fprintf(ofile, "%s\t%d\n", it->word.c_str(), it->freq);
				}
			}
		}

	}

private:
    list<pstring_list>		hzstr_list;
	uint32_t				bucket_num;
	pstring_list			phz_str;
	volatile uint32_t		total_bucket;

	marisa::Trie		mar_trie;
	vector<uint32_t>	weights_all;
	uint32_t		total_word;

	/* sedar trie in sequence */
	int				seq_range_pos;
	int				seq_range_n;
	
	volatile int	steps_done;
	int				thread_n;
	unique_ptr<thread[]> threads;
	mutex			m_var;
	condition_variable	cond_var;

	WrapFile		in_file;
	string			ofile_name;
};

void bucket_run(WordMaker& maker)
{
	const int loop_ever = 1000;
	int curr_loop = 0;

	while(curr_loop < loop_ever)
	{
		Bucket bucket;
		try {
			if(maker.get_bucket(bucket))
			{
				//fprintf(stderr, "bucket starting id=%d size:%d\n", bucket.id, bucket.hz_str->size());
				maker.bucket_process(bucket);
			}
			else
			{
				break;
			}
		}
		catch(...) {
			fprintf(stderr, "error happened hear %d\n", __LINE__);
		}
		curr_loop++;
	}
	if(curr_loop == loop_ever) {
		fprintf(stderr, "error loop forever hear %d\n", __LINE__);
	}
	
	maker.notify_step();
}

void reduce1(WordMaker& maker) {
	maker.reduce_step1_run();
	maker.notify_step();
}

void map_step2(WordMaker& maker, cad_result_list& cad_results) {
	maker.map_step2_run(cad_results);
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
