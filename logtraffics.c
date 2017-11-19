#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <math.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/timeb.h>
#include <sys/stat.h>
#include <sys/dir.h>
#include <errno.h>
#include <sys/utsname.h>

#include <pwd.h>
#include <grp.h>

#include <pthread.h>

#include "zlib.h"

int len_pos = 7;
int time_pos = 16;
int ltime_pos = 4;
int ttype = 0;

time_t tm_to_time(struct tm *tm)
{
    static short monthlen[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    static short monthbegin[12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    time_t          t;      /* return value */

    t  = monthbegin[tm->tm_mon]         /* full months */
         + tm->tm_mday - 1            /* full days */
         + (!(tm->tm_year & 3) && tm->tm_mon > 1);    /* leap day this year*/
    tm->tm_yday = t;
    t += 365 * (tm->tm_year - 70)           /* full years */
         + (tm->tm_year - 69) / 4;        /* past leap days */
    tm->tm_wday = (t + 4) % 7;

    t = t * 86400 + tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;

    if(tm->tm_mday > monthlen[tm->tm_mon] +
       (!(tm->tm_year & 3) && tm->tm_mon == 1)) {
        return((time_t) - 1);
    }

    return(t - 28800); //+8
}

unsigned long tss[576] = {0};
uint64_t bytes[576] = {0};
uint64_t reqs[576] = {0};

int analyze_file(char *file)
{
    printf("Analyze: %s\n", file);
    char line[8192] = {0};
    gzFile in = gzopen(file, "r");

    if(in == NULL) {
        printf("gzopen error L(%d) %s\n", __LINE__, strerror(errno));
        return 1;
    }

    gzbuffer(in, 409600);

    char *p = NULL, *op = NULL, *ippos = NULL, *mon = NULL;
    int pos = 0;
    uint32_t body_len = 0;
    float times = 0;
    char end_tag = '\0';
    int ltime = 0;
    int oldtime = 0;
    int ippos_len = 0;
    long _this_t = 0, a_this_t = 0;
    int atpos = 0;
    struct tm st = {0};

    while(gzgets(in, line, 8092)) {
        pos = 0;
        body_len = 0;
        times = 0;
        ltime = 0;
        p = (char *)(&line);
        strcat(line, " ");

        while(p) {
            if(*p == '"') {
                end_tag = '"';
                op = p + 1;

            } else if(*p == '[') {
                end_tag = ']';
                op = p + 1;

            } else {
                end_tag = ' ';
                op = p;
            }

            if(*p == '-') {
                p++;
                p++;
                pos++;

            } else {
                //p++;

                while(*p && *(++p) != end_tag) {}

                if(!*p) {
                    break;
                }

                if(pos == 0) {
                }

                /*
                if(pos == 0) {
                    end_tag = p[0];
                    p[0] = '\0';
                    ippos = getposbyip(op, &ippos_len);
                    p[0] = end_tag;
                    if(!stristr(ippos, "广东", ippos_len) || !stristr(ippos, "电信", ippos_len)){
                        break;
                    }
                }*/

                p++;

                if(end_tag != ' ') {
                    p++;
                }

                pos++;

                //printf("%d %s\n", pos, op);

                if(pos == ltime_pos) {
                    if(op[0] == '[') {
                        op ++;
                    }

                    st.tm_mday = atoi(op);
                    st.tm_year = atoi(op + 7);
                    st.tm_hour = atoi(op + 12);
                    st.tm_min = atoi(op + 15);
                    st.tm_sec = atoi(op + 18);
                    mon = op + 3;

                    st.tm_mon = 0;

                    switch(mon[0]) {
                        case 'J':
                            if(mon[1] == 'a') {
                                st.tm_mon = 0;

                            } else if(mon[2] == 'n') {
                                st.tm_mon = 5;

                            } else { // Jul
                                st.tm_mon = 6;
                            }

                            break;

                        case 'F':
                            st.tm_mon = 1;
                            break;

                        case 'M':
                            if(mon[2] == 'r') {
                                st.tm_mon = 2;

                            } else {
                                st.tm_mon = 4;
                            }

                            break;

                        case 'A':
                            if(mon[2] == 'r') {
                                st.tm_mon = 3;

                            } else {
                                st.tm_mon = 7;
                            }

                            break;

                        case 'S':
                            st.tm_mon = 8;
                            break;

                        case 'O':
                            st.tm_mon = 9;
                            break;

                        case 'N':
                            st.tm_mon = 10;
                            break;

                        case 'D':
                            st.tm_mon = 11;
                            break;

                        default:
                            break;
                    }

                    st.tm_year -= 1900;
                    _this_t = tm_to_time(&st);
                    a_this_t = _this_t;
                    if(_this_t % 300 != 0) {
                        _this_t += 300 - (_this_t % 300);
                    }

                    atpos = (_this_t % (86400 * 2)) / 300;
                    tss[atpos] = _this_t;
                    reqs[atpos] ++;
                    /*struct tm tm = *localtime(&_this_t);
                    printf("%s\n", line);
                    printf("now: %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
                    exit(0);
                    */

                } else if(pos == len_pos) {
                    body_len = atoi(op);

                } else if(pos == time_pos) {
                    end_tag = p[0];
                    p[0] = '\0';

                    if(ttype == 0) {
                        times = atof(op);

                    } else {
                        times = (float)atoi(op) / 1000;
                    }

                    if ((_this_t % 300 + times) > 300 && times > 1) {
                        times = (int)times + 1;
                        int exceed = _this_t % 300 + times - 300;

                        int body_len_1 = (int)((exceed / times) * body_len);
                        int body_len_2 = body_len - body_len_1;

                        printf("_this_:%ld, times is:%f body_len:%d  atpos:%d  body_len_1:%d  body_len_2:%d, line: %s\n", _this_t, times, body_len, atpos, body_len_1, body_len_2, line);

                        bytes[atpos] += body_len_2;
                        bytes[atpos + 1] += body_len_1;
                    } else {
                        //printf("times is:%f body_len:%d  atpos:%d \n", times, body_len, atpos);
                        bytes[atpos] += body_len;
                    }


                    break;
                }
            }
        }
    }

    gzclose(in);

    return 0;
}

int main(int argc, char *argv[])
{
    printf("From: UPYUN, the next generation CDN service\n");

    int i = 1, j = 0;

    if(argc < 2) {
        printf("./logtraffics file1 file2 ...\n");
        return 1;
    }

    j = atoi(argv[i]);

    if(strlen(argv[i]) < 4 && !strstr(argv[i], "gz") && j > 0) {
        ltime_pos = j;
        i++;
    }

    j = atoi(argv[i]);

    if(strlen(argv[i]) < 4 && !strstr(argv[i], "gz") && j > 0) {
        len_pos = j;
        i++;
    }

    DIR             *dip = NULL, *dip2 = NULL, *inner_dip = NULL, *inner_inner_dip = NULL;
    struct dirent   *dit = NULL, *dit2 = NULL, *inner_dit = NULL, *inner_inner_dit = NULL;
    char path[4096] = {0};
    char file[4096] = {0};
    int nfile = 0;
    struct stat file_stat;
    stat(argv[i], &file_stat);

    if(S_ISDIR(file_stat.st_mode)) {
        if(argv[i][strlen(argv[i]) - 1] != '/') {
            sprintf(path, "%s/", argv[i]);

        } else {
            sprintf(path, "%s", argv[i]);
        }

        if((dip = opendir(path)) != NULL) {
            while((dit = readdir(dip)) != NULL) {
                if(dit->d_name[0] == '.' || dit->d_name[strlen(dit->d_name) - 3] != '.'
                   || dit->d_name[strlen(dit->d_name) - 1] != 'z') {
                    continue;
                }

                sprintf(file, "%s%s", path, dit->d_name);
                analyze_file(file);

                nfile++;
            }
        }

        if(nfile < 1) {
            printf("no gz file in %s\n", path);
            exit(1);
        }

    } else {
        for(; i < argc; i++) {
            char *p = strstr(argv[i], "*");

            char start_p[1024] = {0};
            char end_p[1024] = {0};

            if(p) {
                char *p2 = p;
                int has_p = 0;

                while(p2 > argv[i] && p2--) {
                    if(*p2 == '/') {
                        p2++;
                        has_p = 1;
                        break;
                    }
                }

                if(has_p == 1) {
                    memcpy(path, argv[i], p2 - argv[i]);
                    path[p2 - argv[i]] = '\0';

                    memcpy(start_p, p2, p - p2);

                } else {
                    path[0] = '.';
                    path[1] = '/';
                    path[2] = '\0';

                    memcpy(start_p, argv[i], p - argv[i]);
                }

                memcpy(end_p, p + 1, strlen(argv[i]) - (p - argv[i]) - 1);

                if((dip = opendir(path)) != NULL) {
                    while((dit = readdir(dip)) != NULL) {
                        if(dit->d_name[0] == '.' || dit->d_name[strlen(dit->d_name) - 3] != '.'
                           || dit->d_name[strlen(dit->d_name) - 1] != 'z') {
                            continue;
                        }

                        if(memcmp(dit->d_name, start_p, strlen(start_p)) == 0
                           &&  memcmp(dit->d_name + (strlen(dit->d_name) - strlen(end_p)), end_p, strlen(end_p)) == 0) {
                            sprintf(file, "%s%s", path, dit->d_name);
                            analyze_file(file);
                            nfile++;
                        }
                    }
                }

                if(nfile < 1) {
                    printf("no gz file in %s\n", path);
                    exit(1);
                }

            } else {
                analyze_file(argv[i]);
            }
        }
    }

    printf("Traffic Report:\n");

    struct tm tm = {0};
    unsigned long mini_t = 4070908800;

    for(i = 0; i < 576; i++) {
        if(tss[i] > 0 && tss[i] < mini_t) {
            mini_t = tss[i];
        }
    }

    unsigned long max_t = mini_t + (310 * 288);

    printf("时间\t\t\t流量MB\t带宽mbps\t请求数\n");
    for(; mini_t < max_t; mini_t += 300) {
        for(i = 0; i < 576; i++) {
            if(tss[i] > 0 && tss[i] == mini_t) {
                tm = *localtime(&tss[i]);
                printf("%d-%02d-%02d %02d:%02d:%02d\t%.2f\t%.2f\t%ld\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                       tm.tm_min, tm.tm_sec, (float)(bytes[i] / 1024 / 1024), (float)(bytes[i] * 8 / 300 / 1000 / 1000) * 1.16, reqs[i]);
                break;
            }
        }
    }

    return 0;
}
