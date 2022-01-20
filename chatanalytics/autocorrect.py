import os.path
import re

token_match = re.compile(r"[\w']+|[, ]+")
local_root = os.path.dirname(os.path.abspath(__file__))
alphabet = "abcdefghijklmnopqrstuvwxyz"
first_letter_lookup = {key: [] for key in alphabet}

with open(local_root + "/autocorrect.txt", "r") as f:
    words = f.read().split()
    for word in words:
        first_letter_lookup[word[0]] += [word]


def correct_word(word):
    lookup = first_letter_lookup[word[0]]
    if len(lookup) == 0:
        return 0, word

    closest_word = word
    closest_dist = 3
    for w in lookup:
        d = damerau_levenshtein_distance(word, w)
        if d < closest_dist:
            closest_word = w
            closest_dist = d

    if closest_dist == 3:
        return 0, word
    return closest_dist, closest_word


def correct_passage(passage):
    dist, tokens = 0, []
    for word in token_match.findall(passage):
        if ' ' in word:
            tokens += word
            continue
        d, w = correct_word(word)
        dist += d
        tokens += [w]
    return dist, "".join(tokens)


def damerau_levenshtein_distance(word1, word2, limit=-1):
    # Make a table of costs
    dist = {i:  # think of as the x-axis
                     {j: i+j+2 for j in range(-1, len(word2))}
                 for i in range(-1, len(word1))}
    for i in range(len(word1)):
        for j in range(len(word2)):
            # On insertions to word1: travel vertically on cost table
            insert_cost = dist[i][j-1] + 1
            # On deletions from word1: travel horizontally on cost table
            delete_cost = dist[i-1][j] + 1
            # On substitutions from word1: del+ins, travel diagonally
            sub_cost = dist[i-1][j-1] + (0 if word1[i] == word2[j] else 1)
            min_ex_trans = min(insert_cost, delete_cost, sub_cost)

            if i > 0 and j > 0 and word1[i-1] == word2[j] and word1[i] == word2[j-1]:
                # On transpositions from word1: sub x2, travel diag twice
                trans_cost = dist[i-2][j-2] + 1
                dist[i][j] = min(min_ex_trans, trans_cost)
            else:
                dist[i][j] = min_ex_trans
    return dist[len(word1)-1][len(word2)-1]
