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

#ifndef __NVMF_INTERNAL_H__
#define __NVMF_INTERNAL_H__

#include "spdk/stdinc.h"

#include "spdk/likely.h"
#include "spdk/nvmf.h"
#include "spdk/nvmf_spec.h"
#include "spdk/assert.h"
#include "spdk/bdev.h"
#include "spdk/queue.h"
#include "spdk/util.h"
#include "spdk/thread.h"

#define SPDK_NVMF_MAX_SGL_ENTRIES	16

/* The maximum number of buffers per request */
#define NVMF_REQ_MAX_BUFFERS	(SPDK_NVMF_MAX_SGL_ENTRIES * 2)

/* AIO backend requires block size aligned data buffers,
 * extra 4KiB aligned data buffer should work for most devices.
 */
#define SHIFT_4KB			12u
#define NVMF_DATA_BUFFER_ALIGNMENT	(1u << SHIFT_4KB)
#define NVMF_DATA_BUFFER_MASK		(NVMF_DATA_BUFFER_ALIGNMENT - 1LL)

enum spdk_nvmf_subsystem_state {
	SPDK_NVMF_SUBSYSTEM_INACTIVE = 0,
	SPDK_NVMF_SUBSYSTEM_ACTIVATING,
	SPDK_NVMF_SUBSYSTEM_ACTIVE,
	SPDK_NVMF_SUBSYSTEM_PAUSING,
	SPDK_NVMF_SUBSYSTEM_PAUSED,
	SPDK_NVMF_SUBSYSTEM_RESUMING,
	SPDK_NVMF_SUBSYSTEM_DEACTIVATING,
};

enum spdk_nvmf_qpair_state {
	SPDK_NVMF_QPAIR_UNINITIALIZED = 0,
	SPDK_NVMF_QPAIR_ACTIVE,
	SPDK_NVMF_QPAIR_DEACTIVATING,
	SPDK_NVMF_QPAIR_ERROR,
};

typedef void (*spdk_nvmf_state_change_done)(void *cb_arg, int status);

struct spdk_nvmf_tgt {
	char					name[NVMF_TGT_NAME_MAX_LENGTH];

	uint64_t				discovery_genctr;

	uint32_t				max_subsystems;

	/* Array of subsystem pointers of size max_subsystems indexed by sid */
	struct spdk_nvmf_subsystem		**subsystems;

	TAILQ_HEAD(, spdk_nvmf_transport)	transports;

	spdk_nvmf_tgt_destroy_done_fn		*destroy_cb_fn;
	void					*destroy_cb_arg;

	TAILQ_ENTRY(spdk_nvmf_tgt)		link;
};

struct spdk_nvmf_host {
	char				nqn[SPDK_NVMF_NQN_MAX_LEN + 1];
	TAILQ_ENTRY(spdk_nvmf_host)	link;
};

struct spdk_nvmf_listener {
	struct spdk_nvme_transport_id	trid;
	struct spdk_nvmf_transport	*transport;
	TAILQ_ENTRY(spdk_nvmf_listener)	link;
};

struct spdk_nvmf_transport_pg_cache_buf {
	STAILQ_ENTRY(spdk_nvmf_transport_pg_cache_buf) link;
};

struct spdk_nvmf_transport_poll_group {
	struct spdk_nvmf_transport					*transport;
	/* Requests that are waiting to obtain a data buffer */
	STAILQ_HEAD(, spdk_nvmf_request)				pending_buf_queue;
	STAILQ_HEAD(, spdk_nvmf_transport_pg_cache_buf)			buf_cache;
	uint32_t							buf_cache_count;
	uint32_t							buf_cache_size;
	struct spdk_nvmf_poll_group					*group;
	TAILQ_ENTRY(spdk_nvmf_transport_poll_group)			link;
};

/* Maximum number of registrants supported per namespace */
#define SPDK_NVMF_MAX_NUM_REGISTRANTS		16

struct spdk_nvmf_registrant_info {
	uint64_t		rkey;
	char			host_uuid[SPDK_UUID_STRING_LEN];
};

struct spdk_nvmf_reservation_info {
	bool					ptpl_activated;
	enum spdk_nvme_reservation_type		rtype;
	uint64_t				crkey;
	char					bdev_uuid[SPDK_UUID_STRING_LEN];
	char					holder_uuid[SPDK_UUID_STRING_LEN];
	uint32_t				num_regs;
	struct spdk_nvmf_registrant_info	registrants[SPDK_NVMF_MAX_NUM_REGISTRANTS];
};

struct spdk_nvmf_subsystem_pg_ns_info {
	struct spdk_io_channel		*channel;
	struct spdk_uuid		uuid;
	/* current reservation key, no reservation if the value is 0 */
	uint64_t			crkey;
	/* reservation type */
	enum spdk_nvme_reservation_type	rtype;
	/* Host ID which holds the reservation */
	struct spdk_uuid		holder_id;
	/* Host ID for the registrants with the namespace */
	struct spdk_uuid		reg_hostid[SPDK_NVMF_MAX_NUM_REGISTRANTS];
	uint64_t			num_blocks;
};

struct spdk_nvmf_iosched_ops {
	void*	(*init)(void *ctx);
	void 	(*destroy)(void *ctx);
	void 	(*enqueue)(void *ctx, void *req);
	void*	(*dequeue)(void *ctx);
	void	(*release)(void *req);
	void	(*flush)(void *ctx);
	uint32_t	(*get_credit)(void *ctx);
	void	(*qpair_destroy)(void *ctx);
 };

struct spdk_nvmf_tmgr_rate {
	int64_t target_rate;
	uint64_t last_refill_tsc;
	uint64_t max_bucket_size;
	uint64_t read_tokens;
	uint64_t write_tokens;

	int64_t cpl_rate;
	uint64_t cpl_bytes;
	uint64_t next_cpl_rate_tsc;
	uint64_t cpl_rate_ticks;

	uint64_t total_processed_bytes;
	uint64_t total_completes;
	uint64_t last_stat_bytes;
	uint64_t last_stat_cpls;
};

struct spdk_nvmf_tmgr_lat_cong {
	uint64_t lathresh_ticks;
	uint64_t lathresh_max_ticks;
	uint64_t lathresh_min_ticks;
	uint64_t lathresh_residue;
	uint64_t ewma_latency_ticks;
	uint64_t total_lat_ticks;
	uint64_t total_io;
	uint64_t total_bytes;
	uint64_t last_stat_lat_ticks;
	uint32_t last_stat_io;
	uint64_t last_stat_bytes;

	uint32_t congestion_count;
	uint64_t last_stat_cong;
	uint32_t overloaded_count;
};

struct spdk_nvmf_tmgr {
	int		in_submit;

	struct spdk_nvmf_tmgr_rate	rate;
	struct spdk_nvmf_tmgr_lat_cong	read_cong;
	struct spdk_nvmf_tmgr_lat_cong	write_cong;

	int64_t write_cost;
	int64_t ewma_write_cost;

	uint32_t io_outstanding;
	uint32_t io_waiting;
	uint64_t stat_last_tsc;

	struct spdk_poller		*poller;
	struct spdk_poller		*submit_poller;

	struct spdk_nvmf_iosched_ops	iosched_ops;
	void *sched_ctx;

	TAILQ_HEAD(, spdk_nvmf_request)		read_queued;
	TAILQ_HEAD(, spdk_nvmf_request)		write_queued;
};

typedef void(*spdk_nvmf_poll_group_mod_done)(void *cb_arg, int status);

struct spdk_nvmf_subsystem_poll_group {
	/* Array of namespace information for each namespace indexed by nsid - 1 */
	struct spdk_nvmf_subsystem_pg_ns_info	*ns_info;
	uint32_t				num_ns;

	uint64_t				io_outstanding;
	spdk_nvmf_poll_group_mod_done		cb_fn;
	void					*cb_arg;

	enum spdk_nvmf_subsystem_state		state;

	TAILQ_HEAD(, spdk_nvmf_request)		queued;

	struct spdk_nvmf_tmgr		*tmgr;
};

struct spdk_nvmf_poll_group {
	struct spdk_thread				*thread;
	struct spdk_poller				*poller;

	TAILQ_HEAD(, spdk_nvmf_transport_poll_group)	tgroups;

	/* Array of poll groups indexed by subsystem id (sid) */
	struct spdk_nvmf_subsystem_poll_group		*sgroups;
	uint32_t					num_sgroups;

	/* All of the queue pairs that belong to this poll group */
	TAILQ_HEAD(, spdk_nvmf_qpair)			qpairs;

	/* Statistics */
	struct spdk_nvmf_poll_group_stat		stat;
};

typedef enum _spdk_nvmf_request_exec_status {
	SPDK_NVMF_REQUEST_EXEC_STATUS_COMPLETE,
	SPDK_NVMF_REQUEST_EXEC_STATUS_ASYNCHRONOUS,
} spdk_nvmf_request_exec_status;

union nvmf_h2c_msg {
	struct spdk_nvmf_capsule_cmd			nvmf_cmd;
	struct spdk_nvme_cmd				nvme_cmd;
	struct spdk_nvmf_fabric_prop_set_cmd		prop_set_cmd;
	struct spdk_nvmf_fabric_prop_get_cmd		prop_get_cmd;
	struct spdk_nvmf_fabric_connect_cmd		connect_cmd;
};
SPDK_STATIC_ASSERT(sizeof(union nvmf_h2c_msg) == 64, "Incorrect size");

union nvmf_c2h_msg {
	struct spdk_nvme_cpl				nvme_cpl;
	struct spdk_nvmf_fabric_prop_get_rsp		prop_get_rsp;
	struct spdk_nvmf_fabric_connect_rsp		connect_rsp;
};
SPDK_STATIC_ASSERT(sizeof(union nvmf_c2h_msg) == 16, "Incorrect size");

struct spdk_nvmf_dif_info {
	struct spdk_dif_ctx			dif_ctx;
	bool					dif_insert_or_strip;
	uint32_t				elba_length;
	uint32_t				orig_length;
};

struct spdk_nvmf_request {
	struct spdk_nvmf_qpair		*qpair;
	uint32_t			length;
	enum spdk_nvme_data_transfer	xfer;
	void				*data;
	union nvmf_h2c_msg		*cmd;
	union nvmf_c2h_msg		*rsp;
	void				*buffers[NVMF_REQ_MAX_BUFFERS];
	struct iovec			iov[NVMF_REQ_MAX_BUFFERS];
	uint32_t			iovcnt;
	bool				data_from_pool;
	struct spdk_bdev_io_wait_entry	bdev_io_wait;
	struct spdk_nvmf_dif_info	dif;
	uint64_t			latency_ticks;
	bool				tmgr_enabled;
	uint32_t			vqe_id;

	// for SFQ
	uint64_t			start_vtime;
	uint64_t			finish_vtime;

	STAILQ_ENTRY(spdk_nvmf_request)	buf_link;
	TAILQ_ENTRY(spdk_nvmf_request)	link;
};

struct spdk_nvmf_registrant {
	TAILQ_ENTRY(spdk_nvmf_registrant) link;
	struct spdk_uuid hostid;
	/* Registration key */
	uint64_t rkey;
};

struct spdk_nvmf_ns {
	uint32_t nsid;
	struct spdk_nvmf_subsystem *subsystem;
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *desc;
	struct spdk_nvmf_ns_opts opts;
	/* reservation notificaton mask */
	uint32_t mask;
	/* generation code */
	uint32_t gen;
	/* registrants head */
	TAILQ_HEAD(, spdk_nvmf_registrant) registrants;
	/* current reservation key */
	uint64_t crkey;
	/* reservation type */
	enum spdk_nvme_reservation_type rtype;
	/* current reservation holder, only valid if reservation type can only have one holder */
	struct spdk_nvmf_registrant *holder;
	/* Persist Through Power Loss file which contains the persistent reservation */
	char *ptpl_file;
	/* Persist Through Power Loss feature is enabled */
	bool ptpl_activated;
};

#define PQUEUE_MAX_NUM         4           /* Number of priority queues */
#define QUEUE0_THRESHOLD        16          /* Queue 0: 16 items */
#define QUEUE1_THRESHOLD        4           /* Queue 1: 4 items  */
#define QUEUE2_THRESHOLD        2           /* Queue 2: 2 items  */
#define QUEUE3_THRESHOLD        1           /* Queue 3: 1 items  */
#define BATCH_SIZE              32          /* Maximum processing batch size */
#define EPOCH_THRESHOLD         32          /* Epoch threshold*/
#define MAX_IOS_WAIT_FOR_PRIO	(QUEUE1_THRESHOLD + QUEUE2_THRESHOLD + QUEUE3_THRESHOLD)
#define EPOCH_TIMESLICE_IN_USEC		500000

struct spdk_nvmf_qpair {
	enum spdk_nvmf_qpair_state		state;
	spdk_nvmf_state_change_done		state_cb;
	void					*state_cb_arg;

	struct spdk_nvmf_transport		*transport;
	struct spdk_nvmf_ctrlr			*ctrlr;
	struct spdk_nvmf_poll_group		*group;

	uint16_t				qid;
	uint16_t				sq_head;
	uint16_t				sq_head_max;

	int32_t 				remaining_quota[PQUEUE_MAX_NUM];
	uint64_t 				epoch_last_timeslice;
	uint64_t 				epoch_timeslize_size;
	int32_t 				io_waiting;
	TAILQ_HEAD(, spdk_nvmf_request)		pqueue[PQUEUE_MAX_NUM];

	TAILQ_HEAD(, spdk_nvmf_request)		outstanding;
	TAILQ_ENTRY(spdk_nvmf_qpair)		link;

	struct spdk_nvmf_tmgr		*tmgr;
	void		*iosched_ctx;
	TAILQ_ENTRY(spdk_nvmf_qpair)		iosched_link;
};

struct spdk_nvmf_ctrlr_feat {
	union spdk_nvme_feat_arbitration arbitration;
	union spdk_nvme_feat_power_management power_management;
	union spdk_nvme_feat_error_recovery error_recovery;
	union spdk_nvme_feat_volatile_write_cache volatile_write_cache;
	union spdk_nvme_feat_number_of_queues number_of_queues;
	union spdk_nvme_feat_write_atomicity write_atomicity;
	union spdk_nvme_feat_async_event_configuration async_event_configuration;
	union spdk_nvme_feat_keep_alive_timer keep_alive_timer;
};

/*
 * NVMf reservation notificaton log page.
 */
struct spdk_nvmf_reservation_log {
	struct spdk_nvme_reservation_notification_log	log;
	TAILQ_ENTRY(spdk_nvmf_reservation_log)		link;
	struct spdk_nvmf_ctrlr				*ctrlr;
};

/*
 * This structure represents an NVMe-oF controller,
 * which is like a "session" in networking terms.
 */
struct spdk_nvmf_ctrlr {
	uint16_t			cntlid;
	char				hostnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
	struct spdk_nvmf_subsystem	*subsys;

	struct {
		union spdk_nvme_cap_register	cap;
		union spdk_nvme_vs_register	vs;
		union spdk_nvme_cc_register	cc;
		union spdk_nvme_csts_register	csts;
	} vcprop; /* virtual controller properties */

	struct spdk_nvmf_ctrlr_feat feat;

	struct spdk_nvmf_qpair	*admin_qpair;
	struct spdk_thread	*thread;
	struct spdk_bit_array	*qpair_mask;

	struct spdk_nvmf_request *aer_req;
	union spdk_nvme_async_event_completion notice_event;
	union spdk_nvme_async_event_completion reservation_event;
	struct spdk_uuid  hostid;

	uint16_t changed_ns_list_count;
	struct spdk_nvme_ns_list changed_ns_list;
	uint64_t log_page_count;
	uint8_t num_avail_log_pages;
	TAILQ_HEAD(log_page_head, spdk_nvmf_reservation_log) log_head;

	/* Time to trigger keep-alive--poller_time = now_tick + period */
	uint64_t			last_keep_alive_tick;
	struct spdk_poller		*keep_alive_poller;
	struct spdk_poller		*pqueue_poller;

	bool				dif_insert_or_strip;

	TAILQ_ENTRY(spdk_nvmf_ctrlr)	link;
};

struct spdk_nvmf_subsystem {
	struct spdk_thread		*thread;
	uint32_t			id;
	enum spdk_nvmf_subsystem_state	state;

	char subnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
	enum spdk_nvmf_subtype subtype;
	uint16_t next_cntlid;
	bool allow_any_host;
	bool allow_any_listener ;

	struct spdk_nvmf_tgt			*tgt;

	char sn[SPDK_NVME_CTRLR_SN_LEN + 1];
	char mn[SPDK_NVME_CTRLR_MN_LEN + 1];

	/* Array of pointers to namespaces of size max_nsid indexed by nsid - 1 */
	struct spdk_nvmf_ns			**ns;
	uint32_t				max_nsid;
	/* This is the maximum allowed nsid to a subsystem */
	uint32_t				max_allowed_nsid;

	TAILQ_HEAD(, spdk_nvmf_ctrlr)		ctrlrs;

	TAILQ_HEAD(, spdk_nvmf_host)		hosts;

	TAILQ_HEAD(, spdk_nvmf_listener)	listeners;

	TAILQ_ENTRY(spdk_nvmf_subsystem)	entries;
};

struct spdk_nvmf_transport *spdk_nvmf_tgt_get_transport(struct spdk_nvmf_tgt *tgt,
		enum spdk_nvme_transport_type);

int spdk_nvmf_poll_group_add_transport(struct spdk_nvmf_poll_group *group,
				       struct spdk_nvmf_transport *transport);
int spdk_nvmf_poll_group_update_subsystem(struct spdk_nvmf_poll_group *group,
		struct spdk_nvmf_subsystem *subsystem);
int spdk_nvmf_poll_group_add_subsystem(struct spdk_nvmf_poll_group *group,
				       struct spdk_nvmf_subsystem *subsystem,
				       spdk_nvmf_poll_group_mod_done cb_fn, void *cb_arg);
void spdk_nvmf_poll_group_remove_subsystem(struct spdk_nvmf_poll_group *group,
		struct spdk_nvmf_subsystem *subsystem, spdk_nvmf_poll_group_mod_done cb_fn, void *cb_arg);
void spdk_nvmf_poll_group_pause_subsystem(struct spdk_nvmf_poll_group *group,
		struct spdk_nvmf_subsystem *subsystem, spdk_nvmf_poll_group_mod_done cb_fn, void *cb_arg);
void spdk_nvmf_poll_group_resume_subsystem(struct spdk_nvmf_poll_group *group,
		struct spdk_nvmf_subsystem *subsystem, spdk_nvmf_poll_group_mod_done cb_fn, void *cb_arg);
void spdk_nvmf_request_exec(struct spdk_nvmf_request *req);
int spdk_nvmf_request_free(struct spdk_nvmf_request *req);
int spdk_nvmf_request_complete(struct spdk_nvmf_request *req);

void spdk_nvmf_request_free_buffers(struct spdk_nvmf_request *req,
				    struct spdk_nvmf_transport_poll_group *group,
				    struct spdk_nvmf_transport *transport);
int spdk_nvmf_request_get_buffers(struct spdk_nvmf_request *req,
				  struct spdk_nvmf_transport_poll_group *group,
				  struct spdk_nvmf_transport *transport,
				  uint32_t length);
int spdk_nvmf_request_get_buffers_multi(struct spdk_nvmf_request *req,
					struct spdk_nvmf_transport_poll_group *group,
					struct spdk_nvmf_transport *transport,
					uint32_t *lengths, uint32_t num_lengths);

bool spdk_nvmf_request_get_dif_ctx(struct spdk_nvmf_request *req, struct spdk_dif_ctx *dif_ctx);

void spdk_nvmf_get_discovery_log_page(struct spdk_nvmf_tgt *tgt, const char *hostnqn,
				      struct iovec *iov,
				      uint32_t iovcnt, uint64_t offset, uint32_t length);

void spdk_nvmf_ctrlr_destruct(struct spdk_nvmf_ctrlr *ctrlr);
int spdk_nvmf_ctrlr_process_fabrics_cmd(struct spdk_nvmf_request *req);
int spdk_nvmf_ctrlr_process_admin_cmd(struct spdk_nvmf_request *req);
int spdk_nvmf_ctrlr_process_io_cmd(struct spdk_nvmf_request *req);
int spdk_nvmf_ctrlr_process_io_cmd_prio(struct spdk_nvmf_request *req);
bool spdk_nvmf_ctrlr_dsm_supported(struct spdk_nvmf_ctrlr *ctrlr);
bool spdk_nvmf_ctrlr_write_zeroes_supported(struct spdk_nvmf_ctrlr *ctrlr);
void spdk_nvmf_ctrlr_ns_changed(struct spdk_nvmf_ctrlr *ctrlr, uint32_t nsid);

void spdk_nvmf_tmgr_init(struct spdk_nvmf_tmgr *tmgr);
void spdk_nvmf_tmgr_enable(struct spdk_nvmf_tmgr *tmgr);
void spdk_nvmf_tmgr_disable(struct spdk_nvmf_tmgr *tmgr);
void spdk_nvmf_tmgr_complete(struct spdk_nvmf_request *req);
int spdk_nvmf_tmgr_is_enabled(struct spdk_nvmf_qpair *qpair);
float spdk_nvmf_tmgr_get_ewma_write_cost_float(struct spdk_nvmf_tmgr *tmgr);

void spdk_nvmf_bdev_ctrlr_identify_ns(struct spdk_nvmf_ns *ns, struct spdk_nvme_ns_data *nsdata,
				      bool dif_insert_or_strip);
int spdk_nvmf_bdev_ctrlr_read_cmd(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
				  struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
int spdk_nvmf_bdev_ctrlr_write_cmd(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
				   struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
int spdk_nvmf_bdev_ctrlr_write_zeroes_cmd(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
		struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
int spdk_nvmf_bdev_ctrlr_flush_cmd(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
				   struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
int spdk_nvmf_bdev_ctrlr_dsm_cmd(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
				 struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
int spdk_nvmf_bdev_ctrlr_nvme_passthru_io(struct spdk_bdev *bdev, struct spdk_bdev_desc *desc,
		struct spdk_io_channel *ch, struct spdk_nvmf_request *req);
bool spdk_nvmf_bdev_ctrlr_get_dif_ctx(struct spdk_bdev *bdev, struct spdk_nvme_cmd *cmd,
				      struct spdk_dif_ctx *dif_ctx);

int spdk_nvmf_subsystem_add_ctrlr(struct spdk_nvmf_subsystem *subsystem,
				  struct spdk_nvmf_ctrlr *ctrlr);
void spdk_nvmf_subsystem_remove_ctrlr(struct spdk_nvmf_subsystem *subsystem,
				      struct spdk_nvmf_ctrlr *ctrlr);
struct spdk_nvmf_ctrlr *spdk_nvmf_subsystem_get_ctrlr(struct spdk_nvmf_subsystem *subsystem,
		uint16_t cntlid);
int spdk_nvmf_ctrlr_async_event_ns_notice(struct spdk_nvmf_ctrlr *ctrlr);
void spdk_nvmf_ctrlr_async_event_reservation_notification(struct spdk_nvmf_ctrlr *ctrlr);
void spdk_nvmf_ns_reservation_request(void *ctx);
void spdk_nvmf_ctrlr_reservation_notice_log(struct spdk_nvmf_ctrlr *ctrlr,
		struct spdk_nvmf_ns *ns,
		enum spdk_nvme_reservation_notification_log_page_type type);

/*
 * Abort aer is sent on a per controller basis and sends a completion for the aer to the host.
 * This function should be called when attempting to recover in error paths when it is OK for
 * the host to send a subsequent AER.
 */
void spdk_nvmf_ctrlr_abort_aer(struct spdk_nvmf_ctrlr *ctrlr);

/*
 * Free aer simply frees the rdma resources for the aer without informing the host.
 * This function should be called when deleting a qpair when one wants to make sure
 * the qpair is completely empty before freeing the request. The reason we free the
 * AER without sending a completion is to prevent the host from sending another AER.
 */
void spdk_nvmf_qpair_free_aer(struct spdk_nvmf_qpair *qpair);

static inline struct spdk_nvmf_ns *
_spdk_nvmf_subsystem_get_ns(struct spdk_nvmf_subsystem *subsystem, uint32_t nsid)
{
	/* NOTE: This implicitly also checks for 0, since 0 - 1 wraps around to UINT32_MAX. */
	if (spdk_unlikely(nsid - 1 >= subsystem->max_nsid)) {
		return NULL;
	}

	return subsystem->ns[nsid - 1];
}

static inline bool
spdk_nvmf_qpair_is_admin_queue(struct spdk_nvmf_qpair *qpair)
{
	return qpair->qid == 0;
}

#endif /* __NVMF_INTERNAL_H__ */
