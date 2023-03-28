import json
import gzip


def get_files(total_2017, total_2021):
    '''Creates every necessary filename, cycles through all the files and prints the results'''
    file_2017_base = '/net/corpora/twitter2/Tweets/2017/03/201703'
    file_2021_base = '/net/corpora/twitter2/Tweets/2021/03/202103'
    vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count = 0, 0, 0, 0, 0, 0, 0, 0
    for day in range(7):
        print(f'Day {day+1}')
        day = str(day+8).zfill(2)
        for hour in range(24):
            hour = str(hour).zfill(2)
            file = str(f'{file_2017_base}{day}:{hour}.out.gz')
            total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count = get_text2017(file, total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count)
    print_result2017(total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count)
    vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count = 0, 0, 0, 0, 0, 0, 0, 0,
    for day in range(7):
        print(f'Day {day+1}')
        day = str(day+10).zfill(2)
        for hour in range(24):
            hour = str(hour).zfill(2)
            file = str(f'{file_2021_base}{day}:{hour}.out.gz')
            total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count = get_text2021(file, total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count)
    print_result2021(total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count)


def get_text2017(file, total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count):
    '''Gets text from files'''
    for line in gzip.open(file, "rt"):
        try:
            if '"text":' in line:
                tweet = json.loads(line)['text'].lower()
                total_2017 += 1
                vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count = count_parties2017(tweet.strip(), vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count)
        except:
            continue
    return total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count


def get_text2021(file, total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count):
    '''Gets text from files'''
    for line in gzip.open(file, "rt"):
        try:
            if '"text":' in line:
                tweet = json.loads(line)['text'].encode('utf-8').decode('utf-8', 'ignore').lower()
                total_2021 += 1
                vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count = count_parties2021(tweet.strip(), vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count)
        except:
            continue
    return total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count


def count_parties2017(infile, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count):
    '''Counts the amount of mentions for every party'''
    infile = infile.split(' ')
    vvd_count = vvd_count + sum(infile.count(word) for word in ("vvd", "volkspartij voor vrijheid en democratie"))
    pvv_count = pvv_count + sum(infile.count(word) for word in ("pvv", "partij voor de vrijheid", "partij voor vrijheid", "partij vd vrijheid"))
    cda_count = cda_count + sum(infile.count(word) for word in ("cda", "christen-democratisch appèl", "christen-democratisch appel", "christen democratisch appèl", "christen democratisch Appel"))
    d66_count = d66_count + sum(infile.count(word) for word in ("d66", "democraten 66", "democraten66", "d'66"))
    gl_count = gl_count + sum(infile.count(word) for word in ("gl", "groenlinks", "groen links"))
    sp_count = sp_count + sum(infile.count(word) for word in ("sp", "socialistische partij"))
    pvda_count = pvda_count + sum(infile.count(word) for word in ("pvda", "partij voor de arbeid", "partij vd arbeid", "pvdarbeid", "p vd arbeid", "partijvdarbeid"))
    cu_count = cu_count + sum(infile.count(word) for word in ("cu", "christenunie"))
    return vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count


def count_parties2021(infile, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count):
    '''Counts the amount of mentions for every party'''
    infile = infile.split(' ')
    vvd_count = vvd_count + sum(infile.count(word) for word in ("vvd", "volkspartij voor vrijheid en democratie"))
    d66_count = d66_count + sum(infile.count(word) for word in ("d66", "democraten 66", "democraten66", "d'66"))
    pvv_count = pvv_count + sum(infile.count(word) for word in ("pvv", "partij voor de vrijheid", "partij voor vrijheid", "partij vd vrijheid"))
    cda_count = cda_count + sum(infile.count(word) for word in ("cda", "christen-democratisch appèl", "christen-democratisch appel", "christen democratisch appèl", "christen democratisch Appel"))
    sp_count = sp_count + sum(infile.count(word) for word in ("sp", "socialistische partij"))
    pvda_count = pvda_count + sum(infile.count(word) for word in ("pvda", "partij voor de arbeid", "partij vd arbeid", "pvdarbeid", "p vd arbeid", "partijvdarbeid"))
    gl_count = gl_count + sum(infile.count(word) for word in ("gl", "groenlinks", "groen links"))
    fvd_count = fvd_count + sum(infile.count(word) for word in ("fvd", "forumvdemocratie", "forum voor democratie"))
    return vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count


def print_result2017(total_2017, vvd_count, pvv_count, cda_count, d66_count, gl_count, sp_count, pvda_count, cu_count):
    '''Prints the results'''
    total_mentions = vvd_count + pvv_count + cda_count + d66_count + gl_count + sp_count + pvda_count + cu_count
    print(f'Total tweets: {total_2017}\n\
Total tweets mentioning a party: {total_mentions}\n\
VVD: {vvd_count} Percentage: {vvd_count/total_mentions*100}%\n\
PVV: {pvv_count} Percentage: {pvv_count/total_mentions*100}%\n\
CDA: {cda_count} Percentage: {cda_count/total_mentions*100}%\n\
D66: {d66_count} Percentage: {d66_count/total_mentions*100}%\n\
GroenLinks: {gl_count} Percentage: {gl_count/total_mentions*100}%\n\
SP: {sp_count} Percentage: {sp_count/total_mentions*100}%\n\
PvdA: {pvda_count} Percentage: {pvda_count/total_mentions*100}%\n\
ChristenUnie: {cu_count} Percentage: {cu_count/total_mentions*100}%')


def print_result2021(total_2021, vvd_count, d66_count, pvv_count, cda_count, sp_count, pvda_count, gl_count, fvd_count):
    '''Prints the results'''
    total_mentions = vvd_count + d66_count + pvv_count + cda_count + sp_count + pvda_count + gl_count + fvd_count
    print(f'Total tweets: {total_2021}\n\
Total tweets mentioning a party: {total_mentions}\n\
VVD: Mentions: {vvd_count} Percentage: {vvd_count/total_mentions*100}%\n\
D66: {d66_count} Percentage: {d66_count/total_mentions*100}%\n\
PVV: {pvv_count} Percentage: {pvv_count/total_mentions*100}%\n\
CDA: {cda_count} Percentage: {cda_count/total_mentions*100}%\n\
SP: {sp_count} Percentage: {sp_count/total_mentions*100}%\n\
PvdA: {pvda_count} Percentage: {pvda_count/total_mentions*100}%\n\
GroenLinks: {gl_count} Percentage: {gl_count/total_mentions*100}%\n\
FvD: {fvd_count} Percentage: {fvd_count/total_mentions*100}%')


def main():
    get_files(0, 0)


if __name__ == '__main__':
    main()
