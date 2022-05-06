# Contributor: Divya Maiya (dmaiya@umass.edu), Neha Prakash (nehaprakash@umass.edu)
# Test routine to cleanup files
c = 0
with open("../data/adult.test") as f:
    for line in f:
        c += 1
        file_object = open('../data/adult_test_clean.test', 'a')
        if c == 16281:
            file_object.write(line[:-2] + 'K' + "\n")
        else:
            file_object.write(line[:-2] + "\n")

        file_object.close()

print(c)
