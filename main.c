#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <getopt.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <time.h>
#include <assert.h>
#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_atomic.h>

#define PORT 1234
#define bufsize 1024

#define eval(exp) do { \
						res = exp; \
						if (res) {\
							printf(#exp "; res=%i %s\n",res,fi_strerror(res)); return -1;} \
					 }while(0);

struct fi_info *hints = NULL, *info = NULL;
struct fid_fabric *fabric = NULL;
struct fid_domain *domain = NULL;
struct fid_ep *ep = NULL;
struct fi_av_attr av_attr = {0};
struct fid_av *av = {0};
struct fi_cq_attr cq_attr = {0};
struct fid_cq *cq = NULL;
struct fid_mr *mr = NULL;
fi_addr_t fi_addr[1] = {0};
uint64_t remote_addr = 0;
uint64_t remote_key = 0;
uint64_t remote_desc = 0;

uint64_t value_to_add = 1;
uint64_t result;              // old value of atomic

int server_fd = -1;
pthread_t th_listener = -1;
pthread_t th_server = -1;
char *buf = NULL;
uint64_t my_atomic_value[1] = {0};

char *host = NULL;
char *prov = NULL;
int	 iters = 100;
int	 clients = 1;
bool verbose = false;

static void
close_ofi_resources(void)
{
	if (av)
		fi_close(&av->fid);
	if (cq)
		fi_close(&cq->fid);
	if (ep)
		fi_close(&ep->fid);
	if (domain)
		fi_close(&domain->fid);
	if (fabric)
		fi_close(&fabric->fid);
	if (info)
		fi_freeinfo(info);
	if (hints)
		fi_freeinfo(hints);
}

#define NS_PER_SECOND 1000000000
static inline
uint64_t timespec_delta(struct timespec tstart, struct timespec tend)
{
	int64_t elapsed = ((int64_t)tend.tv_sec - (int64_t)tstart.tv_sec) * (int64_t)NS_PER_SECOND
				+ ((int64_t)tend.tv_nsec - (int64_t)tstart.tv_nsec);

	return elapsed;
}

char *
addr_tostr(void *addr)
{
	char	   *addrstr = NULL;
	size_t		addrlen = 0;

	fi_av_straddr(av, addr, addrstr, &addrlen);
	addrstr = malloc(addrlen);
	fi_av_straddr(av, addr, addrstr, &addrlen);
	return addrstr;
}

static void
sock_send_buf(int sock, void *buf, int len)
{
	send(sock, &len, sizeof(len), 0);
	send(sock, buf, len, 0);
}

static int
sock_recv_buf(int sock, void *buf)
{
	int len;
	read(sock, &len, sizeof(len));
	read(sock, buf, len);
	return len;
}

/* initialize ofi resources and create endpoint using existed hints */
static int
ofi_init_resources(void)
{
	int res;
	struct fi_cq_attr cq_attr = {0};
	struct fi_av_attr av_attr = {0};

#define MR_DEFAULT_MODE (FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR)
	hints->ep_attr->type = FI_EP_RDM;
	hints->caps = FI_RMA|FI_ATOMIC;
	hints->mode = FI_CONTEXT | FI_CONTEXT2 | FI_MSG_PREFIX;
	hints->domain_attr->mr_mode = MR_DEFAULT_MODE;
	hints->fabric_attr->prov_name = prov;
	eval(fi_getinfo(FI_VERSION(2, 4), NULL, NULL, 0, hints, &info));
	if (verbose)
		printf("%s", fi_tostr(info, FI_TYPE_INFO));

	/* create fabric */
	eval(fi_fabric(info->fabric_attr, &fabric, NULL));

	/* create domain */
	eval(fi_domain(fabric, info, &domain, NULL));

	/* create address vector */
	av_attr.type = FI_AV_UNSPEC;
	eval(fi_av_open(domain, &av_attr, &av, NULL));

	/* completion queue */
	cq_attr.size = 0;
	cq_attr.format = FI_CQ_FORMAT_DATA;
	cq_attr.wait_cond = FI_CQ_COND_NONE;
	//cq_attr.wait_obj = FI_WAIT_FD;
	eval(fi_cq_open(domain, &cq_attr, &cq, NULL));

	/* register memory region */
	eval(fi_mr_reg(domain, my_atomic_value, sizeof(uint64_t), FI_REMOTE_READ|FI_REMOTE_WRITE|FI_SEND|FI_RECV, 0, 0, 0, &mr, NULL));

	/* create EP */
	eval(fi_endpoint(domain, info, &ep, NULL));

	/* bind address vector */
	eval(fi_ep_bind(ep, &av->fid, 0));

	/* bind completion queue for inbound completions */
	eval(fi_ep_bind(ep, &cq->fid, FI_RECV|FI_SEND));

	/* enable endpoint */
	eval(fi_enable(ep));
	if (verbose)
		printf("EP initialized\n");
	return 0;
}

static int
av_insert_common(void *addr)
{
	int res;
	res = fi_av_insert(av, addr, 1, fi_addr, 0, NULL);
	if (res != 1)
	{
		fprintf(stderr, "fi_av_insert() error (%i) %s", res, fi_strerror(res));
		return -1;
	}
	else
	{
		char *cli_addr;
		cli_addr = addr_tostr(addr);
		if (verbose)
			printf("inserted address into av: %s\n", cli_addr);
		free(cli_addr);
	}
	return 0;
}

static int
sock_send_ofi_addr(int sock)
{
	unsigned char *mybinaddr = NULL;
	char	   *myhexaddr;
	size_t		addrlen = 0;
	int			res;

	/* determine addr size */
	fi_getname(&ep->fid, mybinaddr, &addrlen);
	/* get local address */
	mybinaddr = malloc(addrlen);
	eval(fi_getname(&ep->fid, mybinaddr, &addrlen));
	sock_send_buf(sock, mybinaddr, addrlen);
	free(mybinaddr);
	return 0;
}

/*
 * Client and server must exchange parameters using sockets
 * before the ofi exchange begins.
 */
static int
ofi_init_worker(int sock, bool am_client)
{
	int res;

	if (am_client)
	{
		buf = (char *)malloc(bufsize);
		hints = fi_allocinfo();
		/* get address format */
		sock_recv_buf(sock, buf);
		hints->addr_format = *(uint32_t *)buf;
		/* get peer address */
		res = sock_recv_buf(sock, buf);
		hints->dest_addrlen = res;
		hints->dest_addr = malloc(res);
		memcpy(hints->dest_addr, buf, res);
		/* get peer mr address */
		sock_recv_buf(sock, buf);
		remote_addr = *(uint64_t *)buf;
		/* get peer rkey */
		sock_recv_buf(sock, buf);
		remote_key = *(uint64_t *)buf;
		/* get peer mr desc */
		sock_recv_buf(sock, buf);
		remote_desc = *(uint64_t *)buf;
		res = ofi_init_resources();
		av_insert_common(hints->dest_addr);
		/* send my address */
		sock_send_ofi_addr(sock);
		if (verbose)
			printf("client started\n");
		return res;
	}
	else
	{
		char *cli_addr;
		uint64_t desc = (uint64_t)fi_mr_desc(mr);
		uint64_t base_addr = (info->domain_attr->mr_mode & FI_MR_VIRT_ADDR) ?
					(uint64_t) my_atomic_value : 0;

		remote_key = fi_mr_key(mr);
		/* send address format */
		sock_send_buf(sock, &info->addr_format, sizeof(info->addr_format));
		/* send my address */
		sock_send_ofi_addr(sock);
		/* send mr address */
		sock_send_buf(sock, &base_addr, sizeof(base_addr));
		/* send rkey */
		sock_send_buf(sock, &remote_key, sizeof(remote_key));
		/* send mr desc */
		sock_send_buf(sock, &desc, sizeof(desc));
		/* get peer address */
		sock_recv_buf(sock, buf);
		av_insert_common(buf);
		cli_addr = addr_tostr(buf);
		if (verbose)
			printf("client accepted: %s\n", cli_addr);
		free(cli_addr);
		return 0;
	}
}

/* send atomic operation request and wait for it's completion */
int
atomic_fetch(void)
{
	ssize_t res;
	struct fi_cq_data_entry comp;

	do
	{
		/*
		 * Remote:
		 * 1. receives remote_addr, value_to_add
		 * 2. oldvalue = remote_addr
		 *    remote_addr = oldvalue + value_to_add
		 * 3. sends oldvalue
		 */
		res = fi_fetch_atomic(
					ep,
					&value_to_add,    // op data
					1,
					fi_mr_desc(mr),
					&result,
					(void *)remote_desc,
					fi_addr[0],
					remote_addr,
					remote_key,
					FI_UINT64,
					FI_SUM,
					NULL);
	} while (res == -FI_EAGAIN);

	/* wait for completion */
	res = fi_cq_read(cq, &comp, 1);
	while(res <= 0)
		res = fi_cq_read(cq, &comp, 1);
	assert(res == 1);
	if (verbose)
		printf("===> CQ: result=%lu Op completed len=%lu flags=%lu\n", result, comp.len, comp.flags);
	return 0;
}

void
client_routine(char* host)
{
	int sock = 0;
	struct sockaddr_in serv_addr;
	int flag = 1;
	int i;
	uint64_t sum = 0;
	struct timespec t_begin;
	struct timespec t_end;
	double elapsed;
	double periter;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(PORT);

	if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0)
	{
		fprintf(stderr, "invalid address\n");
		exit(1);
	}

	if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	{
		fprintf(stderr, "can't connect\n");
		exit(1);
	}
	if (ofi_init_worker(sock, true) != 0)
	{
		fprintf(stderr, "can't init ofi\n");
		exit(1);
	}
	close(sock);

	/* main counter loop */
	clock_gettime(CLOCK_MONOTONIC, &t_begin);
	for(i = 0; i < iters; i++)
	{
		if (atomic_fetch() != -1)
			sum += result;
	}
	clock_gettime(CLOCK_MONOTONIC, &t_end);

	elapsed = (double)timespec_delta(t_begin, t_end)/NS_PER_SECOND;
	periter = (double)timespec_delta(t_begin, t_end)/(iters * 1000);
	printf("\ntime      usec/iter    last val    sum\n");
	printf("%-10.4f%-13.4f%-12lu%lu\n", elapsed, periter, result, sum);
	close_ofi_resources();
	exit(0);
}

void *
server_thread(void* arg)
{
	ssize_t res;
	struct fi_cq_data_entry comp;
	struct timespec t_begin, t_current;

	fprintf(stdout, "Server thread started\n");
	fprintf(stdout, "Provider: %s\n", prov);
	fprintf(stdout, "current value=%lu", my_atomic_value[0]);
	fflush(stdout);
	clock_gettime(CLOCK_MONOTONIC, &t_begin);
	while (true)
	{
		clock_gettime(CLOCK_MONOTONIC, &t_current);
		if ((double)timespec_delta(t_begin, t_current) >= NS_PER_SECOND)
		{
			clock_gettime(CLOCK_MONOTONIC, &t_begin);
			fprintf(stdout, "\rcurrent value=%lu", my_atomic_value[0]);
			fflush(stdout);
		}
		res = fi_cq_read(cq, &comp, 1);
		switch(res)
		{
			case 0:
				break;
			case 1:
				if (verbose)
					printf("===> CQ: result=%lu Op completed len=%lu flags=%lu\n", result, comp.len, comp.flags);
				break;
			case -FI_EAVAIL:
				fprintf(stderr, "OFI error available\n");
				break;
			case -FI_EAGAIN:
				break;
			default:
				printf("fi_cq_read() = (%li) %s\n", res, fi_strerror(res));
				exit(1);
				break;
		}
	}
	return NULL;
}

/* server-side thread that just accepts new clients */
void *listener_thread(void* arg)
{
	int cli_count = 0;
	int flag = 1;
	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in address = {AF_INET, htons(PORT), INADDR_ANY};

	setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
	if (verbose)
		printf("listener thread started\n");

	if (0 != bind(server_fd, (struct sockaddr *)&address, sizeof(address)))
	{
		fprintf(stderr, "bind error: %s\n", strerror(errno));
		exit(1);
	}

	if (0 != listen(server_fd, SOMAXCONN))
	{
		fprintf(stderr, "listen error: %s\n", strerror(errno));
		exit(1);
	}

	while (true)
	{
		int client_fd;
		pthread_t client_th;

		client_fd = accept(server_fd, NULL, NULL);		
		if (verbose)
		{
			printf("\nclient thread #%lu started\n", client_th);
			fflush(stdout);
		}
		ofi_init_worker(client_fd, false);
		close(client_fd);

	}
	return NULL;
}

static void
parse_args(int argc, char *argv[])
{
	int opt;

	while ((opt = getopt(argc, argv, "h:p:i:c:v")) != -1) {
		switch (opt) {
			case 'h':
				host = strdup(optarg);
				break;
			case 'p':
				prov = strdup(optarg);
				break;
			case 'i':
				iters = atoi(optarg);
				break;
			case 'c':
				clients = atoi(optarg);
				break;
			case 'v':
				verbose = true;
				break;
			default:
				fprintf(stderr, "Usage: %s [-h host] [-p provider] [-i iters] [-c clients] [-v]\n", argv[0]);
				exit(EXIT_FAILURE);
				break;
		}
	}
	if (!prov)
		prov = strdup("psm3");
}

int main(int argc, char *argv[])
{
	struct timespec t_begin;
	struct timespec t_end;
	double elapsed;

	parse_args(argc, argv);

	if (!host)
	{
		buf = (char *)malloc(bufsize);
		hints = fi_allocinfo();
		if (ofi_init_resources() != 0)
		{
			fprintf(stderr, "can't init ofi\n");
			exit(1);
		}
		pthread_create(&th_listener, NULL, listener_thread, NULL);
		pthread_create(&th_server, NULL, server_thread, NULL);
		pthread_join(th_server, NULL);
	}
	else
	{
		int i;
		pid_t pid = 0;
		printf("Provider: %s\n", prov);
		printf("Iters: %i\n", iters);
		printf("Clients: %i\n---\n", clients);

		clock_gettime(CLOCK_MONOTONIC, &t_begin);
		for (i = 0;i < clients; i++)
		{
			pid = fork();
			if (pid == 0)
			{
				client_routine(host);
				break;
			}
		}
		if (pid != 0)
		{
			while (wait(&i) > 0)
			{
				/* nop */
			};
			clock_gettime(CLOCK_MONOTONIC, &t_end);
			elapsed = (double)timespec_delta(t_begin, t_end)/NS_PER_SECOND;
			if (clients > 1)
				printf("---\noverall time: %.4f\n", elapsed);
		}

	}
	return 0;
}
