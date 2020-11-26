/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "nvmf_internal.h"
#include "transport.h"

#include "spdk/bit_array.h"
#include "spdk/endian.h"
#include "spdk/thread.h"
#include "spdk/trace.h"
#include "spdk/nvme_spec.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/version.h"

#include "spdk_internal/log.h"

#define SPDK_NVMF_TMGR_CWND_MAX_IOCOUNT 1024
#define SPDK_NVMF_TMGR_CWND_SIZE_MIN (4096UL)
#define SPDK_NVMF_TMGR_RATE_QUANTUM (131072UL)
#define SPDK_NVMF_TMGR_LATHRESH_MAX_USEC 1500
#define SPDK_NVMF_TMGR_LATHRESH_MIN_USEC 250
#define SPDK_NVMF_TMGR_NUM_FLOW_MAX 0xFFFF

#define SPDK_NVMF_TMGR_EWMA_PARAM 	  1	// Weight = 1/(2^SPDK_NVMF_TMGR_EWMA_PARAM)
#define SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM 4	// Weight = 1/(2^SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM)

#define SPDK_NVMF_TMGR_WEIGHT_PARAM (10)
#define SPDK_NVMF_TMGR_WEIGHT_ONE (1<<SPDK_NVMF_TMGR_WEIGHT_PARAM)
#define SPDK_NVMF_TMGR_WC_BASELINE (9*SPDK_NVMF_TMGR_WEIGHT_ONE)
#define SPDK_NVMF_TMGR_WC_DESCFACTOR (SPDK_NVMF_TMGR_WEIGHT_ONE/2)
#define SPDK_NVMF_TMGR_CONG_PARAM (10)

enum spdk_nvmf_tmgr_rate_state {
	SPDK_NVMF_TMGR_RATE_SUBMITTABLE,
	SPDK_NVMF_TMGR_RATE_SUBMITTABLE_READONLY,
	SPDK_NVMF_TMGR_RATE_DEFERRED,
	SPDK_NVMF_TMGR_RATE_OVERLOADED,
	SPDK_NVMF_TMGR_RATE_CONGESTION,
	SPDK_NVMF_TMGR_RATE_SLOWSTART,
	SPDK_NVMF_TMGR_RATE_DRAINING,
	SPDK_NVMF_TMGR_RATE_OK
};

enum spdk_nvmf_tmgr_iosched_type {
	SPDK_NVMF_TMGR_IOSCHED_DRR,
	SPDK_NVMF_TMGR_IOSCHED_WDRR,
	SPDK_NVMF_TMGR_IOSCHED_NOOP
};

#define SPDK_NVMF_TMGR_EWMA(ewma, raw, param) \
	((raw >> param) + (ewma) - (ewma >> param))

static inline void
spdk_nvmf_tmgr_lat_cong_init(struct spdk_nvmf_tmgr_lat_cong *cong)
{
	cong->ewma_latency_ticks = 0;
	cong->lathresh_max_ticks = SPDK_NVMF_TMGR_LATHRESH_MAX_USEC * spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;
	cong->lathresh_min_ticks = SPDK_NVMF_TMGR_LATHRESH_MIN_USEC * spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;
	cong->lathresh_ticks = cong->lathresh_max_ticks;
	cong->lathresh_residue = 0;
}

static inline void
spdk_nvmf_tmgr_rate_init(struct spdk_nvmf_tmgr_rate *rate)
{
	rate->last_refill_tsc = spdk_get_ticks();
	rate->target_rate = (1000UL) * 1024 * 1024;
	rate->read_tokens = rate->write_tokens = rate->max_bucket_size = 256*1024;

	rate->cpl_rate = 0;
	rate->cpl_bytes = 0;
	rate->cpl_rate_ticks = spdk_get_ticks_hz() / 10000;

	rate->last_stat_bytes = 0;
	rate->total_processed_bytes = 0;
}

static inline int
spdk_nvmf_tmgr_cong_update(struct spdk_nvmf_tmgr_lat_cong *cong, uint64_t iolen, uint64_t latency_ticks)
{
	cong->ewma_latency_ticks = SPDK_NVMF_TMGR_EWMA(cong->ewma_latency_ticks,
										 latency_ticks, SPDK_NVMF_TMGR_EWMA_PARAM);

	cong->total_io++;
	cong->total_bytes += iolen;
	cong->total_lat_ticks += latency_ticks;
	if (cong->ewma_latency_ticks > cong->lathresh_max_ticks) {
		cong->lathresh_ticks = (cong->lathresh_ticks
						+ cong->lathresh_max_ticks) / 2;
		cong->lathresh_residue = 0;
		cong->congestion_count++;
		cong->overloaded_count++;
		return SPDK_NVMF_TMGR_RATE_OVERLOADED;
	} else if (cong->ewma_latency_ticks > cong->lathresh_ticks) {
		cong->lathresh_ticks = (cong->lathresh_ticks
						+ cong->lathresh_max_ticks) / 2;
		cong->lathresh_residue = 0;
		cong->congestion_count++;
		return SPDK_NVMF_TMGR_RATE_CONGESTION;
	} else if (cong->ewma_latency_ticks > cong->lathresh_min_ticks) {
		// EWMA-style (or low-pass filter) latency threshold (Weight = 1/2^13).
		// multiply by IO Length
		cong->lathresh_residue += (cong->lathresh_ticks - cong->ewma_latency_ticks);
		cong->lathresh_ticks -= cong->lathresh_residue >> SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM;
		cong->lathresh_residue = cong->lathresh_residue & ((1 << SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM) - 1);
	} else {
		cong->lathresh_residue += (cong->lathresh_ticks - cong->ewma_latency_ticks);
		cong->lathresh_ticks -= cong->lathresh_residue >> SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM;
		cong->lathresh_residue = cong->lathresh_residue & ((1 << SPDK_NVMF_TMGR_LATHRES_EWMA_PARAM) - 1);
		return SPDK_NVMF_TMGR_RATE_SLOWSTART;
	}
	return SPDK_NVMF_TMGR_RATE_OK;
}

static inline void
spdk_nvmf_tmgr_rate_bucket_refill(struct spdk_nvmf_tmgr *tmgr, uint64_t now)
{
	struct spdk_nvmf_tmgr_rate *rate = &tmgr->rate;

	uint64_t delta_ticks = now - rate->last_refill_tsc;
	uint64_t token_rate = rate->target_rate * delta_ticks / spdk_get_ticks_hz();

	rate->read_tokens += token_rate * tmgr->write_cost / (tmgr->write_cost + SPDK_NVMF_TMGR_WEIGHT_ONE);
	rate->write_tokens += token_rate * SPDK_NVMF_TMGR_WEIGHT_ONE / (tmgr->write_cost + SPDK_NVMF_TMGR_WEIGHT_ONE);

	if (rate->read_tokens > rate->max_bucket_size) {
		rate->write_tokens = spdk_min(rate->write_tokens + rate->read_tokens - rate->max_bucket_size, rate->max_bucket_size);
		rate->read_tokens = rate->max_bucket_size;
	}
	if (rate->write_tokens > rate->max_bucket_size) {
		rate->read_tokens = spdk_min(rate->read_tokens + rate->write_tokens - rate->max_bucket_size, rate->max_bucket_size);
		rate->write_tokens = rate->max_bucket_size;
	}
	rate->last_refill_tsc = now;
	return;
}

static inline int
spdk_nvmf_tmgr_rate_is_submittable(struct spdk_nvmf_tmgr *tmgr, struct spdk_nvmf_request *req) 
{
	struct spdk_nvmf_tmgr_rate *rate = &tmgr->rate;
	struct spdk_nvme_cmd *cmd = &req->cmd->nvme_cmd;

	if (cmd->opc == SPDK_NVME_OPC_WRITE && rate->write_tokens >= req->length) {
		return SPDK_NVMF_TMGR_RATE_SUBMITTABLE;
	} else if (cmd->opc != SPDK_NVME_OPC_WRITE && rate->read_tokens >= req->length) {
		return SPDK_NVMF_TMGR_RATE_SUBMITTABLE;
	} else {
		return SPDK_NVMF_TMGR_RATE_DEFERRED;
	}
}

static inline void
spdk_nvmf_tmgr_rate_submit(struct spdk_nvmf_tmgr *tmgr, struct spdk_nvmf_request *req) 
{
	struct spdk_nvmf_tmgr_rate *rate = &tmgr->rate;
	struct spdk_nvme_cmd *cmd = &req->cmd->nvme_cmd;

	if (cmd->opc == SPDK_NVME_OPC_WRITE) {
		rate->write_tokens -= req->length;
	} else {
		rate->read_tokens -= req->length;
	}
	return;
}

static inline void
spdk_nvmf_tmgr_rate_complete(struct spdk_nvmf_tmgr *tmgr, uint64_t iolen)
{
	struct spdk_nvmf_tmgr_rate *rate = &tmgr->rate;
	uint64_t now;
	rate->total_processed_bytes += iolen;
	rate->total_completes++;
	rate->cpl_bytes += iolen;

	if (rate->cpl_bytes > 16777216) {
		now = spdk_get_ticks();
		rate->cpl_rate = rate->cpl_bytes * spdk_get_ticks_hz() / (now - rate->next_cpl_rate_tsc);
		rate->next_cpl_rate_tsc = now; 
		rate->cpl_bytes = 0;
	}
}

static inline void
spdk_nvmf_tmgr_update_write_cost(struct spdk_nvmf_tmgr *tmgr, uint64_t write_cost)
{
	tmgr->write_cost = write_cost;
	if (tmgr->write_cost > SPDK_NVMF_TMGR_WC_BASELINE)
		tmgr->write_cost = SPDK_NVMF_TMGR_WC_BASELINE;
	else if (tmgr->write_cost <= SPDK_NVMF_TMGR_WEIGHT_ONE)
		tmgr->write_cost = SPDK_NVMF_TMGR_WEIGHT_ONE;
	return;
}

float
spdk_nvmf_tmgr_get_ewma_write_cost_float(struct spdk_nvmf_tmgr *tmgr)
{
	return (float)tmgr->ewma_write_cost / SPDK_NVMF_TMGR_WEIGHT_ONE;
}

static void
spdk_nvmf_tmgr_submit(struct spdk_nvmf_tmgr *tmgr)
{
	int		status, ret;
	struct spdk_nvmf_request *req, *tmp;
	uint64_t now = spdk_get_ticks();

	spdk_nvmf_tmgr_rate_bucket_refill(tmgr, now);
	tmgr->in_submit = true;

	// we process deferred requests first 
	TAILQ_FOREACH_SAFE(req, &tmgr->read_queued, link, tmp) {
		if (spdk_nvmf_tmgr_rate_is_submittable(tmgr, req)
				== SPDK_NVMF_TMGR_RATE_SUBMITTABLE) {
			spdk_nvmf_tmgr_rate_submit(tmgr, req);
			TAILQ_REMOVE(&tmgr->read_queued, req, link);
			TAILQ_INSERT_TAIL(&req->qpair->outstanding, req, link);
			tmgr->io_outstanding += req->length;
			tmgr->io_waiting--;
			status = spdk_nvmf_ctrlr_process_io_cmd(req);
			if (status == SPDK_NVMF_REQUEST_EXEC_STATUS_COMPLETE) {
				spdk_nvmf_request_complete(req);
			}
		} else {
			break;
		}
	}
	if (!TAILQ_EMPTY(&tmgr->read_queued) && !TAILQ_EMPTY(&tmgr->write_queued)) {
		tmgr->in_submit = false;
		return;
	}

	// if no requests in the deffered queue, we dequeue using io scheduler
	while (1) {
		if ((req = tmgr->iosched_ops.dequeue(tmgr->sched_ctx)) == NULL) {
			break;
		}

		ret = spdk_nvmf_tmgr_rate_is_submittable(tmgr, req);
		if (ret == SPDK_NVMF_TMGR_RATE_DEFERRED) {
			TAILQ_INSERT_TAIL(&tmgr->read_queued, req, link);
			break;
		}

		spdk_nvmf_tmgr_rate_submit(tmgr, req);
		tmgr->io_outstanding += req->length;
		tmgr->io_waiting--;
		TAILQ_INSERT_TAIL(&req->qpair->outstanding, req, link);
		status = spdk_nvmf_ctrlr_process_io_cmd(req);
		if (status == SPDK_NVMF_REQUEST_EXEC_STATUS_COMPLETE) {
			spdk_nvmf_request_complete(req);
		}
	}

	tmgr->in_submit = false;
	return;
}

void
spdk_nvmf_tmgr_complete(struct spdk_nvmf_request *req)
{
	struct spdk_nvme_cmd *cmd = &req->cmd->nvme_cmd;
	struct spdk_nvme_cpl *rsp = &req->rsp->nvme_cpl;
	struct spdk_nvmf_qpair *qpair;
	struct spdk_nvmf_subsystem_poll_group *sgroup = NULL;
	struct spdk_nvmf_tmgr *tmgr;
	struct spdk_nvmf_tmgr_lat_cong *cong;
	int res;

	qpair = req->qpair;
	assert(qpair->ctrlr);
	sgroup = &qpair->group->sgroups[qpair->ctrlr->subsys->id];
	tmgr = sgroup->tmgr;

	if (cmd->opc == SPDK_NVME_OPC_WRITE) {
		cong = &tmgr->write_cong;
	} else {
		cong = &tmgr->read_cong;
	}

	tmgr->io_outstanding -= req->length;
	spdk_nvmf_tmgr_rate_complete(tmgr, req->length);
	tmgr->iosched_ops.release(req);

	if (rsp->status.sct == SPDK_NVME_SCT_GENERIC &&
			rsp->status.sc == SPDK_NVME_SC_SUCCESS) {
		res = spdk_nvmf_tmgr_cong_update(cong, req->length, req->latency_ticks);
		if (res == SPDK_NVMF_TMGR_RATE_OVERLOADED) {
			if (tmgr->rate.target_rate > tmgr->rate.cpl_rate) {
				tmgr->rate.target_rate = tmgr->rate.cpl_rate;
			}
			tmgr->rate.write_tokens = 0;
			tmgr->rate.read_tokens = 0;
			tmgr->rate.target_rate -= req->length;
		} else if (res == SPDK_NVMF_TMGR_RATE_CONGESTION) {
			tmgr->rate.target_rate -= req->length;
		} else if (res == SPDK_NVMF_TMGR_RATE_SLOWSTART) {
			tmgr->rate.target_rate += 8*req->length;
		} else {
			tmgr->rate.target_rate += req->length;
		}

		tmgr->rate.target_rate = spdk_max(tmgr->rate.target_rate, 52428800);
		tmgr->rate.target_rate = spdk_min(tmgr->rate.target_rate, 4294967296L);
		rsp->rsvd1 = (cmd->rsvd2 << 16) + spdk_min((1 << 16) - 1, tmgr->iosched_ops.get_credit(req));
	}

	if (!tmgr->in_submit) {
		spdk_nvmf_tmgr_submit(tmgr);
	}
	return;
}

static void *spdk_nvmf_iosched_noop_init(void *ctx)
{
	return ctx;
}

static void spdk_nvmf_iosched_noop_destroy(void *ctx)
{
	return;
}

static void spdk_nvmf_iosched_noop_enqueue(void *ctx, void *item)
{
	int status;
	struct spdk_nvmf_request *req = item;

	TAILQ_INSERT_TAIL(&req->qpair->outstanding, req, link);
	status = spdk_nvmf_ctrlr_process_io_cmd(req);
	if (status == SPDK_NVMF_REQUEST_EXEC_STATUS_COMPLETE) {
		spdk_nvmf_request_complete(req);
	}

	return;
}

static void *spdk_nvmf_iosched_noop_dequeue(void *ctx)
{
	return NULL;
}

static void spdk_nvmf_iosched_noop_release(void *item)
{
	return;
}

static void spdk_nvmf_iosched_noop_flush(void *ctx)
{
	return;
}

static uint32_t spdk_nvmf_iosched_noop_get_credit(void *ctx)
{
	return UINT32_MAX;
}

static void spdk_nvmf_iosched_noop_qpair_destroy(void *ctx)
{
	return;
}

static const struct spdk_nvmf_iosched_ops noop_ioched_ops = {
	.init 			= spdk_nvmf_iosched_noop_init,
	.destroy 		= spdk_nvmf_iosched_noop_destroy,
	.enqueue 		= spdk_nvmf_iosched_noop_enqueue,
	.dequeue 		= spdk_nvmf_iosched_noop_dequeue,
	.release 		= spdk_nvmf_iosched_noop_release,
	.flush			= spdk_nvmf_iosched_noop_flush,
	.get_credit		= spdk_nvmf_iosched_noop_get_credit,
	.qpair_destroy 	= spdk_nvmf_iosched_noop_qpair_destroy
};

#define SPDK_NVMF_TMGR_DRR_NUM_VQE (8+1)
#define SPDK_NVMF_TMGR_DRR_MAX_VQE_SIZE (131072)

struct spdk_nvmf_iosched_drr_vqe {
	uint16_t			id;
	uint16_t			submits;
	uint16_t			completions;
	uint32_t			length;

	TAILQ_ENTRY(spdk_nvmf_iosched_drr_vqe)	link;
};

struct spdk_nvmf_iosched_drr_qpair_ctx {
	int		paused;

	uint64_t	round;
	uint32_t 	quantum;
	int64_t 	deficit;
	uint64_t 	processing;
	uint64_t 	backlog;
	int16_t		vqsize;

	uint64_t	vqcount;

	uint32_t	idle_count;
	uint32_t	deferred_count;
	uint64_t	deferred_enter;

	int32_t	credit_per_vslot;
	int32_t io_wait;
	int32_t io_processing;

	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx;
	struct spdk_nvmf_iosched_drr_vqe 	vqe[SPDK_NVMF_TMGR_DRR_NUM_VQE];
	struct spdk_nvmf_iosched_drr_vqe	*curr_vqe;
	TAILQ_HEAD(, spdk_nvmf_iosched_drr_vqe)		vq;
	TAILQ_HEAD(, spdk_nvmf_request)		queued;
	
	uint64_t 	temp_data;
};

struct spdk_nvmf_iosched_drr_sched_ctx {
	struct spdk_nvmf_tmgr *tmgr;

	uint32_t 	num_active_qpairs;
	uint32_t 	num_deferred_qpairs;

	uint64_t	global_quantum;
	uint64_t	target_rate;
	uint64_t	rate_window_ticks;
	uint64_t	last_window_ticks;
	uint32_t	deficit_incr_count;

	uint64_t	last_sched_tsc;
	int16_t		vqmaxqd;

	struct spdk_nvmf_qpair 				*curr_qpair;	
	TAILQ_HEAD(, spdk_nvmf_qpair)			active_qpairs;
	TAILQ_HEAD(, spdk_nvmf_qpair)			deferred_qpairs;
	TAILQ_HEAD(, spdk_nvmf_qpair)			idle_qpairs;
};

static void *spdk_nvmf_iosched_wdrr_init(void *ctx)
{
	struct spdk_nvmf_tmgr *tmgr = ctx;
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx;

	sched_ctx = calloc(1, sizeof(struct spdk_nvmf_iosched_drr_sched_ctx));
	sched_ctx->tmgr = tmgr;
	sched_ctx->num_active_qpairs = 0;
	sched_ctx->vqmaxqd = 1;
	sched_ctx->curr_qpair = NULL;
	TAILQ_INIT(&sched_ctx->active_qpairs);
	TAILQ_INIT(&sched_ctx->deferred_qpairs);
	TAILQ_INIT(&sched_ctx->idle_qpairs);
	return sched_ctx;
}

static void
spdk_nvmf_iosched_wdrr_qpair_destroy(void *ctx)
{
	struct spdk_nvmf_qpair	*qpair = ctx;
	struct spdk_nvmf_qpair	*tq_qpair, *tmp_qpair;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx = qpair->iosched_ctx;
	struct spdk_nvmf_tmgr		*tmgr = qpair->tmgr;
	struct spdk_nvmf_iosched_drr_sched_ctx	*sched_ctx = tmgr->sched_ctx;
	struct spdk_nvmf_request	*req, *tmp;

	if (!qpair_ctx) {
		return;
	}

	TAILQ_FOREACH_SAFE(req, &qpair_ctx->queued, link, tmp) {
		TAILQ_REMOVE(&qpair_ctx->queued, req, link);
		if (spdk_nvmf_transport_req_free(req)) {
			SPDK_ERRLOG("Transport request free error!\n");
		}
	}

	if (sched_ctx->curr_qpair == qpair) {
		sched_ctx->curr_qpair = TAILQ_NEXT(qpair, iosched_link);
	}

	TAILQ_FOREACH_SAFE(tq_qpair, &sched_ctx->active_qpairs, iosched_link, tmp_qpair) {
		if (tq_qpair == qpair) {
			
			TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
			SPDK_DEBUGLOG(SPDK_LOG_NVMF, "active qpairs removed\n");
		}
	}

	TAILQ_FOREACH_SAFE(tq_qpair, &sched_ctx->deferred_qpairs, iosched_link, tmp_qpair) {
		if (tq_qpair == qpair) {
			TAILQ_REMOVE(&sched_ctx->deferred_qpairs, qpair, iosched_link);
		}
	}

	TAILQ_FOREACH_SAFE(tq_qpair, &sched_ctx->idle_qpairs, iosched_link, tmp_qpair) {
		if (tq_qpair == qpair) {
			TAILQ_REMOVE(&sched_ctx->idle_qpairs, qpair, iosched_link);
		}
	}

	SPDK_DEBUGLOG(SPDK_LOG_NVMF, "qid(%u) round(%lu) deferred(%u) idle(%u) vqcount(%lu) (active:%u)\n",
		 qpair->qid, qpair_ctx->round, qpair_ctx->deferred_count, qpair_ctx->idle_count,
		 qpair_ctx->vqcount, sched_ctx->num_active_qpairs);
	free(qpair->iosched_ctx);
	qpair->iosched_ctx = NULL;
	return;
}

static void spdk_nvmf_iosched_wdrr_destroy(void *ctx)
{
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_qpair		*qpair;

	while (!TAILQ_EMPTY(&sched_ctx->active_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->active_qpairs);
		TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
		spdk_nvmf_iosched_wdrr_qpair_destroy(qpair);
	}
	while (!TAILQ_EMPTY(&sched_ctx->deferred_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->deferred_qpairs);
		TAILQ_REMOVE(&sched_ctx->deferred_qpairs, qpair, iosched_link);
		spdk_nvmf_iosched_wdrr_qpair_destroy(qpair);
	}
	while (!TAILQ_EMPTY(&sched_ctx->idle_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->idle_qpairs);
		TAILQ_REMOVE(&sched_ctx->idle_qpairs, qpair, iosched_link);
		spdk_nvmf_iosched_wdrr_qpair_destroy(qpair);
	}
		
	if (tmgr->sched_ctx) {
		free(tmgr->sched_ctx);
		tmgr->sched_ctx = NULL;
	}

	return;
}

#define SPDK_NVMF_IOSCHED_WDRR_VSLOT_CREDIT_MAX 32

static void spdk_nvmf_iosched_wdrr_enqueue(void *ctx, void *item)
{
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_request 	*req = item;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_qpair		*qpair = req->qpair;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx;
	struct spdk_nvmf_iosched_drr_vqe		*vqe;
	int 	i;

	if (spdk_unlikely(qpair->iosched_ctx == NULL)) {
		qpair->iosched_ctx = calloc(1, sizeof(struct spdk_nvmf_iosched_drr_qpair_ctx));
		qpair_ctx = qpair->iosched_ctx;
		qpair_ctx->sched_ctx = sched_ctx;
		TAILQ_INIT(&qpair_ctx->vq);
		qpair_ctx->credit_per_vslot = SPDK_NVMF_IOSCHED_WDRR_VSLOT_CREDIT_MAX;
		qpair_ctx->io_wait = 0;
		qpair_ctx->vqsize = 1;
		qpair_ctx->deficit = qpair_ctx->quantum = SPDK_NVMF_TMGR_RATE_QUANTUM;
		qpair_ctx->curr_vqe = &qpair_ctx->vqe[0];
		for (i=0; i < SPDK_NVMF_TMGR_DRR_NUM_VQE; i++) {
			vqe = &qpair_ctx->vqe[i];
			vqe->id = i;
			TAILQ_INSERT_TAIL(&qpair_ctx->vq, vqe, link);
		}

		TAILQ_INIT(&qpair_ctx->queued);
		TAILQ_INSERT_TAIL(&sched_ctx->idle_qpairs, qpair, iosched_link);
	} else {
		qpair_ctx = qpair->iosched_ctx;
	}

	TAILQ_INSERT_TAIL(&qpair_ctx->queued, req, link);
	if (!qpair_ctx->backlog && !qpair_ctx->deferred_enter) {
		TAILQ_REMOVE(&sched_ctx->idle_qpairs, qpair, iosched_link);
		TAILQ_INSERT_TAIL(&sched_ctx->active_qpairs, qpair, iosched_link);
		if (!qpair_ctx->io_processing) {
			sched_ctx->num_active_qpairs++;
			sched_ctx->vqmaxqd = spdk_max(1, (SPDK_NVMF_TMGR_DRR_NUM_VQE-1)/sched_ctx->num_active_qpairs);
			qpair_ctx->deficit += qpair_ctx->quantum;
		}
	}
	qpair_ctx->backlog += req->length;
	qpair_ctx->io_wait++;
	tmgr->io_waiting++;
	spdk_nvmf_tmgr_submit(tmgr);
	return;
}

static void *spdk_nvmf_iosched_wdrr_dequeue(void *ctx)
{
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_request 	*req = NULL;
	struct spdk_nvmf_qpair		*qpair, *tmp_qpair;
	struct spdk_nvme_cmd		*cmd;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx;
	int64_t	weighted_length;
	int64_t write_cost = tmgr->write_cost;

	if (TAILQ_EMPTY(&sched_ctx->active_qpairs)) {
		return NULL;
	}

	TAILQ_FOREACH_SAFE(qpair, &sched_ctx->active_qpairs, iosched_link, tmp_qpair) {
		qpair_ctx = qpair->iosched_ctx;
		qpair_ctx->round++;
		req = TAILQ_FIRST(&qpair_ctx->queued);
		cmd = &req->cmd->nvme_cmd;
		weighted_length = req->length;
		if (cmd->opc == SPDK_NVME_OPC_WRITE) {
			weighted_length = (write_cost * weighted_length) >> SPDK_NVMF_TMGR_WEIGHT_PARAM;
		}

		if (qpair_ctx->deficit < weighted_length) {
			req = NULL;
			qpair_ctx->deficit += qpair_ctx->quantum;
			if (!TAILQ_NEXT(qpair, iosched_link)) { // this is already a tail
				tmp_qpair = qpair;
			} else {
				TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
				TAILQ_INSERT_TAIL(&sched_ctx->active_qpairs, qpair, iosched_link);
			}
			continue;
		}

		qpair_ctx->curr_vqe->length += weighted_length;
		qpair_ctx->curr_vqe->submits++;
		qpair_ctx->backlog -= req->length;
		qpair_ctx->processing += req->length;
		qpair_ctx->io_processing++;
		qpair_ctx->io_wait--;
		qpair_ctx->deficit -= weighted_length;
		req->vqe_id = qpair_ctx->curr_vqe->id;
		TAILQ_REMOVE(&qpair_ctx->queued, req, link);

		if (qpair_ctx->curr_vqe->length	// this allows oversized v-slot
					>= SPDK_NVMF_TMGR_DRR_MAX_VQE_SIZE) {
			// Find a new slot if the current request is not fit in the opened slot
			TAILQ_REMOVE(&qpair_ctx->vq, qpair_ctx->curr_vqe, link);
			TAILQ_INSERT_TAIL(&qpair_ctx->vq, qpair_ctx->curr_vqe, link);
			if (sched_ctx->vqmaxqd - qpair_ctx->vqsize <= 0) {
				TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
				TAILQ_INSERT_TAIL(&sched_ctx->deferred_qpairs, qpair, iosched_link);
				qpair_ctx->curr_vqe = NULL;
				qpair_ctx->deferred_count++;
				qpair_ctx->deferred_enter = qpair_ctx->deferred_count;
				qpair_ctx->deficit = 0;
			} else {
				qpair_ctx->vqsize++;
				qpair_ctx->vqcount++;
				qpair_ctx->curr_vqe = TAILQ_FIRST(&qpair_ctx->vq);
				assert(qpair_ctx->curr_vqe->length == 0);
			}
		}
		if (!qpair_ctx->deferred_enter && qpair_ctx->backlog == 0) {
			qpair_ctx->deficit = 0;
			TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
			TAILQ_INSERT_TAIL(&sched_ctx->idle_qpairs, qpair, iosched_link);
		}
		break;
	}
	return req;
}

static void spdk_nvmf_iosched_wdrr_flush(void *ctx)
{
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_qpair		*qpair;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx;

	TAILQ_FOREACH(qpair, &sched_ctx->active_qpairs, iosched_link) {
			qpair_ctx = qpair->iosched_ctx;
			qpair_ctx->deficit = SPDK_NVMF_TMGR_RATE_QUANTUM;
	}

	TAILQ_FOREACH(qpair, &sched_ctx->deferred_qpairs, iosched_link) {
			qpair_ctx = qpair->iosched_ctx;
			qpair_ctx->deficit = SPDK_NVMF_TMGR_RATE_QUANTUM;
	}

	return;
}

static void spdk_nvmf_iosched_wdrr_release(void *item)
{
	struct spdk_nvmf_request *req = item;
	struct spdk_nvmf_qpair		*qpair;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx;
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx;
	struct spdk_nvmf_iosched_drr_vqe		*vqe;
	qpair = req->qpair;
	qpair_ctx = qpair->iosched_ctx;
	sched_ctx = qpair_ctx->sched_ctx;
	vqe = &qpair_ctx->vqe[req->vqe_id];

	vqe->completions++;
	if ((vqe->submits == vqe->completions) && (qpair_ctx->curr_vqe != vqe)) {
		qpair_ctx->vqsize--;
		qpair_ctx->credit_per_vslot = vqe->submits;
		vqe->submits = 0;
		vqe->completions = 0;
		vqe->length = 0;
		TAILQ_REMOVE(&qpair_ctx->vq, vqe, link);
		TAILQ_INSERT_HEAD(&qpair_ctx->vq, vqe, link);
		if (qpair_ctx->deferred_enter && (sched_ctx->vqmaxqd - qpair_ctx->vqsize > 0)) {
			TAILQ_REMOVE(&sched_ctx->deferred_qpairs, qpair, iosched_link);
			if (qpair_ctx->backlog) {
				// move this qpair to processing queue
				TAILQ_INSERT_TAIL(&sched_ctx->active_qpairs, qpair, iosched_link);
			} else {
				// qpair is idle
				TAILQ_INSERT_TAIL(&sched_ctx->idle_qpairs, qpair, iosched_link);
			}
			qpair_ctx->curr_vqe = TAILQ_FIRST(&qpair_ctx->vq);
			assert(qpair_ctx->curr_vqe->length == 0);
			qpair_ctx->vqsize++;
			qpair_ctx->vqcount++;
			qpair_ctx->deferred_enter = 0;
		}
	}
	qpair_ctx->io_processing--;
	qpair_ctx->processing -= req->length;
	qpair_ctx->temp_data += req->length;
	if (!qpair_ctx->backlog && !qpair_ctx->io_processing && !qpair_ctx->deferred_enter) {
		qpair_ctx->idle_count++;
		sched_ctx->num_active_qpairs--;
		if (sched_ctx->num_active_qpairs == 0) {
			sched_ctx->vqmaxqd = SPDK_NVMF_TMGR_DRR_NUM_VQE-1;
		} else {
			sched_ctx->vqmaxqd = spdk_max(1, (SPDK_NVMF_TMGR_DRR_NUM_VQE-1)/sched_ctx->num_active_qpairs);
		}
	}
	return;
}

static uint32_t spdk_nvmf_iosched_wdrr_get_credit(void *ctx)
{
	struct spdk_nvmf_request 	*req = ctx;
	struct spdk_nvmf_qpair		*qpair;
	struct spdk_nvmf_iosched_drr_qpair_ctx *qpair_ctx;
	struct spdk_nvmf_iosched_drr_sched_ctx *sched_ctx;
	int32_t		credit_limit;

	qpair = req->qpair;
	qpair_ctx = qpair->iosched_ctx;
	sched_ctx = qpair_ctx->sched_ctx;
	credit_limit = sched_ctx->vqmaxqd * qpair_ctx->credit_per_vslot;
	return credit_limit;
	if (qpair_ctx->io_wait + qpair_ctx->io_processing > credit_limit) {
		return (qpair_ctx->io_wait ? 0 : 1);
	} else {
		return spdk_max(1, credit_limit - qpair_ctx->io_wait - qpair_ctx->io_processing);
	}
}

static const struct spdk_nvmf_iosched_ops wdrr_ioched_ops = {
	.init 			= spdk_nvmf_iosched_wdrr_init,
	.destroy 		= spdk_nvmf_iosched_wdrr_destroy,
	.enqueue 		= spdk_nvmf_iosched_wdrr_enqueue,
	.dequeue 		= spdk_nvmf_iosched_wdrr_dequeue,
	.release 		= spdk_nvmf_iosched_wdrr_release,
	.flush			= spdk_nvmf_iosched_wdrr_flush,
	.get_credit		= spdk_nvmf_iosched_wdrr_get_credit,
	.qpair_destroy 	= spdk_nvmf_iosched_wdrr_qpair_destroy
};

static int
spdk_nvmf_tmgr_submit_poller(void *arg)
{
	struct spdk_nvmf_tmgr *tmgr = arg;
	if (tmgr->io_outstanding <= 131072) {
		spdk_nvmf_tmgr_submit(tmgr);
	}
	return 0;
}

static int
spdk_nvmf_tmgr_poller(void *arg)
{
	struct spdk_nvmf_tmgr *tmgr = arg;
	uint64_t rio, wio, wr_avg_lat;
	uint64_t now = spdk_get_ticks();
	rio = tmgr->read_cong.total_io - tmgr->read_cong.last_stat_io;
	wio = tmgr->write_cong.total_io - tmgr->write_cong.last_stat_io;
	if (wio) {
		wr_avg_lat = (tmgr->write_cong.total_io - tmgr->write_cong.last_stat_io) / wio;
	} else {
		wr_avg_lat = 0;
	}

	if ((rio && (tmgr->write_cong.lathresh_min_ticks < wr_avg_lat))
			 || wr_avg_lat > tmgr->write_cong.lathresh_max_ticks) {
		spdk_nvmf_tmgr_update_write_cost(tmgr, (tmgr->write_cost+SPDK_NVMF_TMGR_WC_BASELINE)/2);
	} else {
		spdk_nvmf_tmgr_update_write_cost(tmgr, tmgr->write_cost - SPDK_NVMF_TMGR_WC_DESCFACTOR);
	}
		
	tmgr->rate.last_stat_bytes = tmgr->rate.total_processed_bytes;
	tmgr->rate.last_stat_cpls = tmgr->rate.total_completes;
	tmgr->read_cong.last_stat_io = tmgr->read_cong.total_io;
	tmgr->read_cong.last_stat_bytes = tmgr->read_cong.total_bytes;
	tmgr->read_cong.last_stat_lat_ticks = tmgr->read_cong.total_lat_ticks;
	tmgr->write_cong.last_stat_io = tmgr->write_cong.total_io;
	tmgr->write_cong.last_stat_bytes = tmgr->write_cong.total_bytes;
	tmgr->write_cong.last_stat_lat_ticks = tmgr->write_cong.total_lat_ticks;
	tmgr->stat_last_tsc = now;

	return 0;
}

void
spdk_nvmf_tmgr_init(struct spdk_nvmf_tmgr *tmgr)
{
	TAILQ_INIT(&tmgr->read_queued);
	TAILQ_INIT(&tmgr->write_queued);
	tmgr->in_submit = false;
	tmgr->poller = NULL;

	SPDK_NOTICELOG("System tick rate: %llu/usec\n", spdk_get_ticks_hz()/SPDK_SEC_TO_USEC);

	spdk_nvmf_tmgr_rate_init(&tmgr->rate);
	spdk_nvmf_tmgr_lat_cong_init(&tmgr->read_cong);
	spdk_nvmf_tmgr_lat_cong_init(&tmgr->write_cong);

	tmgr->iosched_ops = noop_ioched_ops;
	tmgr->iosched_ops = wdrr_ioched_ops;

	tmgr->write_cost = SPDK_NVMF_TMGR_WC_BASELINE;
	return;
}

void  
spdk_nvmf_tmgr_enable(struct spdk_nvmf_tmgr *tmgr)
{
	tmgr->sched_ctx = tmgr->iosched_ops.init(tmgr);
	tmgr->poller = spdk_poller_register(spdk_nvmf_tmgr_poller, tmgr, 100000);
	tmgr->submit_poller = spdk_poller_register(spdk_nvmf_tmgr_submit_poller, tmgr, 1000);
	return;
}

void
spdk_nvmf_tmgr_disable(struct spdk_nvmf_tmgr *tmgr)
{
	if (tmgr->poller) {
		spdk_poller_unregister(&tmgr->poller);
		tmgr->poller = NULL;
	}
	if (tmgr->submit_poller) {
		spdk_poller_unregister(&tmgr->submit_poller);
		tmgr->submit_poller = NULL;
	}

	return;
}
