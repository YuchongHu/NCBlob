// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "ErasureCodeLonse.h"


extern "C" {
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"
#include "cauchy.h"
#include "liberation.h"
}

#define LARGEST_VECTOR_WORDSIZE 16

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

using std::ostream;
using std::map;
using std::set;

using std::make_pair;
using std::pair;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::ErasureCodeProfile;

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeLonse: ";
}


int ErasureCodeLonse::init(ErasureCodeProfile& profile, ostream *ss)
{
  int err = 0;
  // dout(10) << "technique=" << technique << dendl;
  // profile["technique"] = technique;
  err |= parse(profile, ss);
  if (err)
    return err;
  // prepare();
  // init _encode_matrix
  generate_matrix(_encode_matrix, k+m, k, 8);
  // init _repair_matrix
  generate_matrix(_repair_matrix, origin_m, origin_k + origin_m - 1, 8);

  return ErasureCode::init(profile, ss);
}

void ErasureCodeLonse::generate_matrix(int* matrix, int rows, int cols, int w) {
    using namespace std;
    //rows = (k+m)*m
    //cols = k*m
    int k = cols;
    int n = rows;

    memset(matrix, 0, rows * cols * sizeof(int));

    for (int i=0; i<rows; i++) {
        int tmp = 1;
        for (int j=0; j<cols; j++) {
            matrix[i*cols+j] = tmp;
            tmp = Computation::singleMulti(tmp, i+1, w);
        }
    }


}

int ErasureCodeLonse::parse(ErasureCodeProfile &profile,
			       ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &origin_k, DEFAULT_K, ss);
  err |= to_int("m", profile, &origin_m, DEFAULT_M, ss);
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  k = origin_k*origin_m;
  m = origin_m*origin_m;
  if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + m) {
    *ss << "mapping " << profile.find("mapping")->second
	<< " maps " << chunk_mapping.size() << " chunks instead of"
	<< " the expected " << k + m << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
  }
  err |= sanity_check_k_m(k, m, ss);
  return err;
}

unsigned int ErasureCodeLonse::get_chunk_size(unsigned int object_size) const
{
  //??????
  // unsigned alignment = get_alignment();
  unsigned alignment = k*w*sizeof(int);
  if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*LARGEST_VECTOR_WORDSIZE;
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0 );
  ceph_assert(padded_length % k == 0);
  return padded_length / k;
  
}

int ErasureCodeLonse::minimum_to_decode(const set<int> &want_to_read,
				       const set<int> &available,
				       map<int, vector<pair<int, int>>> *minimum)
{
  dout(0) << "minimum_to_decode invoked" << dendl;
  return ErasureCode::minimum_to_decode(want_to_read, available, minimum);
}

int ErasureCodeLonse::encode_chunks(const set<int> &want_to_encode,
				       map<int, bufferlist> *encoded)
{
  size_t data_size = (*encoded)[0].length();
  char ** coding_ptrs = static_cast<char **>(malloc((k+m) * sizeof(char *)));
  char *chunks[k];
  for (int i = 0; i < k; i++)
    chunks[i] = (*encoded)[i].c_str();
  // jerasure_encode(&chunks[0], &chunks[k], (*encoded)[0].length());
  jerasure_matrix_encode(k, k+m, 8, _encode_matrix, &chunks[0], coding_ptrs, (*encoded)[0].length());
  
  // copy the n*m code blocks into encoded
  for (int i = 0; i < k+m; i++){
    (*encoded)[i].clear();
    (*encoded)[i].append(coding_ptrs+i, data_size);
  }
  return 0;
}

int ErasureCodeLonse::decode_chunks(const set<int> &want_to_read,
				       const map<int, bufferlist> &chunks,
				       map<int, bufferlist> *decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m];
  int* tmp_matrix = (int*)malloc((k+m-1)*(k*m)*sizeof(int));
  int cnt = 0;

  // the n-1 slices from code blocks
  // char ** coding_slice = static_cast<char **>(malloc((origin_k + origin_m - 1) * sizeof(char *)));
  
  char *coding_slice[origin_k+origin_m-1];
  char *new_coding[origin_m];
  
//   for (int i = 0; i < origin_k + origin_m - 1; i++) {
//       coding_slice[i] = static_cast<char *>(malloc(blocksize));
// //        coding_ptrs[i] = "xkjklwjfaf";
//   }

  for (int i =  0; i < origin_k + origin_m; i++) {
    if (chunks.find(i*origin_m) == chunks.end()) {
      for(int j = 0; j < origin_m; j++){
        new_coding[j] = (*decoded)[i*origin_m+j].c_str();
      }
      erasures[erasures_count] = i;
      erasures_count++;
      continue;
    }
    int row = i * origin_m * (origin_k *origin_m) + row_idx * origin_k * origin_m; // 每m行选第_row_idx行为coef，这里的_row_idx是咋选的
    memcpy(tmp_matrix + (cnt*origin_k*origin_m),
               _encode_matrix+(row), // 从row开始复制_k*_m
               sizeof(int)*origin_k*origin_m); // 复制一行_k*_m
    
    // memcpy(*(coding_slice+cnt), (*decoded)[i*origin_m+row_idx].c_str(), blocksize);
    coding_slice[cnt] = (*decoded)[i*origin_m+row_idx].c_str();

    cnt++;
    // if (i < k)
    //   data[i] = (*decoded)[i].c_str();
    // else
    //   coding[i - k] = (*decoded)[i].c_str();
  }
  // new_encode_matrix是用来填充的
  int *new_encode_matrix = jerasure_matrix_multiply(_repair_matrix, tmp_matrix,
                                                    origin_m, origin_k+origin_m-1,
                                                    origin_k+origin_m-1, origin_k*origin_m,
                                                    8);
  // update _encode_matrix
  // 为新生成的块更新编码矩阵，即用生成的new_encode_matrix填充进原来encode_matrix坏掉的块的对应的m行
  for (int i = 0; i < origin_m; i++) {
      int row = lostidx*origin_m*origin_k*origin_m + i*origin_m*origin_k;
//        for (int j = 0; j < k*m; j++){
//            cout << new_encode_matrix[i*k*m+j] << "\t";
//            cout << "\t";
//        }
//        cout<<endl;
      memcpy(_encode_matrix+row,
              new_encode_matrix + (i * origin_k * origin_m),
              sizeof(int) * origin_k * origin_m);
  }

  /**repair lost chunks*/


  char ** new_coding = static_cast<char **>(malloc(origin_m * sizeof(char *)));
  for (int i = 0; i < origin_m; i++) {
      new_coding[i] = static_cast<char *>(malloc(blocksize));
  }

  for (int i = 0; i < origin_m; i++){
      int dest_id = i+(origin_k+origin_m-1);
      int matrix_row = i * (origin_k+origin_m-1);
      jerasure_matrix_dotprod(origin_k+origin_m-1, 8, _repair_matrix+matrix_row, NULL, dest_id, coding_slice, new_coding, blocksize);
  }

  for(int i = 0; i < origin_m; i++)
        memcpy(*(coding_ptrs+(lostidx*m+i)), *(new_coding+i), size);




  erasures[erasures_count] = -1;

  ceph_assert(erasures_count > 0);
  return jerasure_decode(erasures, data, coding, blocksize);
}

bool ErasureCodeJerasure::is_prime(int value)
{
  int prime55[] = {
    2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,
    73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,
    151,157,163,167,173,179,
    181,191,193,197,199,211,223,227,229,233,239,241,251,257
  };
  int i;
  for (i = 0; i < 55; i++)
    if (value == prime55[i])
      return true;
  return false;
}

// 
// ErasureCodeJerasureReedSolomonVandermonde
//
void ErasureCodeJerasureReedSolomonVandermonde::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  jerasure_matrix_encode(k, m, w, matrix, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonVandermonde::jerasure_decode(int *erasures,
                                                                char **data,
                                                                char **coding,
                                                                int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1,
				erasures, data, coding, blocksize);
}

unsigned ErasureCodeJerasureReedSolomonVandermonde::get_alignment() const
{
  if (per_chunk_alignment) {
    return w * LARGEST_VECTOR_WORDSIZE;
  } else {
    unsigned alignment = k*w*sizeof(int);
    if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }
}

int ErasureCodeJerasureReedSolomonVandermonde::parse(ErasureCodeProfile &profile,
						     ostream *ss)
{
  int err = 0;
  err |= ErasureCodeJerasure::parse(profile, ss);
  if (w != 8 && w != 16 && w != 32) {
    *ss << "ReedSolomonVandermonde: w=" << w
	<< " must be one of {8, 16, 32} : revert to " << DEFAULT_W << std::endl;
    err = -EINVAL;
  }
  err |= to_bool("jerasure-per-chunk-alignment", profile,
		 &per_chunk_alignment, "false", ss);
  return err;
}

void ErasureCodeJerasureReedSolomonVandermonde::prepare()
{
  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
}

// 
// ErasureCodeJerasureReedSolomonRAID6
//
void ErasureCodeJerasureReedSolomonRAID6::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  reed_sol_r6_encode(k, w, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonRAID6::jerasure_decode(int *erasures,
							 char **data,
							 char **coding,
							 int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1, erasures, data, coding, blocksize);
}

unsigned ErasureCodeJerasureReedSolomonRAID6::get_alignment() const
{
  if (per_chunk_alignment) {
    return w * LARGEST_VECTOR_WORDSIZE;
  } else {
    unsigned alignment = k*w*sizeof(int);
    if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }
}

int ErasureCodeJerasureReedSolomonRAID6::parse(ErasureCodeProfile &profile,
					       ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (m != stoi(DEFAULT_M)) {
    *ss << "ReedSolomonRAID6: m=" << m
        << " must be 2 for RAID6: revert to 2" << std::endl;
    err = -EINVAL;
  }
  if (w != 8 && w != 16 && w != 32) {
    *ss << "ReedSolomonRAID6: w=" << w
	<< " must be one of {8, 16, 32} : revert to 8 " << std::endl;
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureReedSolomonRAID6::prepare()
{
  matrix = reed_sol_r6_coding_matrix(k, w);
}

// 
// ErasureCodeJerasureCauchy
//
void ErasureCodeJerasureCauchy::jerasure_encode(char **data,
						char **coding,
						int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule,
			   data, coding, blocksize, packetsize);
}

int ErasureCodeJerasureCauchy::jerasure_decode(int *erasures,
					       char **data,
					       char **coding,
					       int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix,
				       erasures, data, coding, blocksize, packetsize, 1);
}

unsigned ErasureCodeJerasureCauchy::get_alignment() const
{
  if (per_chunk_alignment) {
    unsigned alignment = w * packetsize;
    unsigned modulo = alignment % LARGEST_VECTOR_WORDSIZE;
    if (modulo)
      alignment += LARGEST_VECTOR_WORDSIZE - modulo;
    return alignment;
  } else {
    unsigned alignment = k*w*packetsize*sizeof(int);
    if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }  
}

int ErasureCodeJerasureCauchy::parse(ErasureCodeProfile &profile,
				     ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);
  err |= to_bool("jerasure-per-chunk-alignment", profile,
		 &per_chunk_alignment, "false", ss);
  return err;
}

void ErasureCodeJerasureCauchy::prepare_schedule(int *matrix)
{
  bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, matrix);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

ErasureCodeJerasureCauchy::~ErasureCodeJerasureCauchy() 
{
  if (bitmatrix)
    free(bitmatrix);
  if (schedule)
    jerasure_free_schedule(schedule);
}

// 
// ErasureCodeJerasureCauchyOrig
//
void ErasureCodeJerasureCauchyOrig::prepare()
{
  int *matrix = cauchy_original_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureCauchyGood
//
void ErasureCodeJerasureCauchyGood::prepare()
{
  int *matrix = cauchy_good_general_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureLiberation
//
ErasureCodeJerasureLiberation::~ErasureCodeJerasureLiberation()
{
  if (bitmatrix)
    free(bitmatrix);
  if (schedule)
    jerasure_free_schedule(schedule);
}

void ErasureCodeJerasureLiberation::jerasure_encode(char **data,
                                                    char **coding,
                                                    int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule, data,
			   coding, blocksize, packetsize);
}

int ErasureCodeJerasureLiberation::jerasure_decode(int *erasures,
                                                    char **data,
                                                    char **coding,
                                                    int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix, erasures, data,
				       coding, blocksize, packetsize, 1);
}

unsigned ErasureCodeJerasureLiberation::get_alignment() const
{
  unsigned alignment = k*w*packetsize*sizeof(int);
  if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

bool ErasureCodeJerasureLiberation::check_k(ostream *ss) const
{
  if (k > w) {
    *ss << "k=" << k << " must be less than or equal to w=" << w << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_w(ostream *ss) const
{
  if (w <= 2 || !is_prime(w)) {
    *ss <<  "w=" << w << " must be greater than two and be prime" << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_packetsize_set(ostream *ss) const
{
  if (packetsize == 0) {
    *ss << "packetsize=" << packetsize << " must be set" << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_packetsize(ostream *ss) const
{
  if ((packetsize%(sizeof(int))) != 0) {
    *ss << "packetsize=" << packetsize
	<< " must be a multiple of sizeof(int) = " << sizeof(int) << std::endl;
    return false;
  } else {
    return true;
  }
}

int ErasureCodeJerasureLiberation::revert_to_default(ErasureCodeProfile &profile,
						     ostream *ss)
{
  int err = 0;
  *ss << "reverting to k=" << DEFAULT_K << ", w="
      << DEFAULT_W << ", packetsize=" << DEFAULT_PACKETSIZE << std::endl;
  profile["k"] = DEFAULT_K;
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  profile["w"] = DEFAULT_W;
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  profile["packetsize"] = DEFAULT_PACKETSIZE;
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);
  return err;
}

int ErasureCodeJerasureLiberation::parse(ErasureCodeProfile &profile,
					 ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);

  bool error = false;
  if (!check_k(ss))
    error = true;
  if (!check_w(ss))
    error = true;
  if (!check_packetsize_set(ss) || !check_packetsize(ss))
    error = true;
  if (error) {
    revert_to_default(profile, ss);
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureLiberation::prepare()
{
  bitmatrix = liberation_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureBlaumRoth
//
bool ErasureCodeJerasureBlaumRoth::check_w(ostream *ss) const
{
  // back in Firefly, w = 7 was the default and produced usable
  // chunks. Tolerate this value for backward compatibility.
  if (w == 7)
    return true;
  if (w <= 2 || !is_prime(w+1)) {
    *ss <<  "w=" << w << " must be greater than two and "
	<< "w+1 must be prime" << std::endl;
    return false;
  } else {
    return true;
  }
}

void ErasureCodeJerasureBlaumRoth::prepare()
{
  bitmatrix = blaum_roth_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureLiber8tion
//
int ErasureCodeJerasureLiber8tion::parse(ErasureCodeProfile &profile,
					 ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (m != stoi(DEFAULT_M)) {
    *ss << "liber8tion: m=" << m << " must be " << DEFAULT_M
        << " for liber8tion: revert to " << DEFAULT_M << std::endl;
    err = -EINVAL;
  }
  if (w != stoi(DEFAULT_W)) {
    *ss << "liber8tion: w=" << w << " must be " << DEFAULT_W
        << " for liber8tion: revert to " << DEFAULT_W << std::endl;
    err = -EINVAL;
  }
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);

  bool error = false;
  if (!check_k(ss))
    error = true;
  if (!check_packetsize_set(ss))
    error = true;
  if (error) {
    revert_to_default(profile, ss);
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureLiber8tion::prepare()
{
  bitmatrix = liber8tion_coding_bitmatrix(k);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
}
