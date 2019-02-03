# sanitize the data in combined imdb to msid mapping txt

with open('./combined_msid_to_imdb_mapping_4k_02022019.txt', 'r') as fr, open('./sanitized_combined_msid_to_imdb_mapping_4k_02022019.txt', 'w') as fw:
    lines = fr.readlines()
    for line in lines:
        try:
            token1, token2 = line.strip('\n').split('\t')
        except:
            
            continue
        
        if token1.startswith('nm') and token2.startswith('m.'):
            print("{}\t{}".format(token1, token2), file=fw)
        elif token2.startswith('nm') and token1.startswith('m.'):
            print("{}\t{}".format(token2, token1), file=fw)

    print('done')

