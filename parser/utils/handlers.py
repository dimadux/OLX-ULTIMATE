import re

from keras.preprocessing.text import  text_to_word_sequence

def clean_text(text_list):

    text_list = [text_to_word_sequence(text) for text in text_list]
    text_list = [filter(lambda x: x != "\r", text) for text in text_list]
    text_list = [" ".join(text) for text in text_list]
    return " ".join(text_list)

def clean_group_tags(tags_list):
    data = ["".join(filter(lambda x: x not in ["\n", "\t"], item)) for item in tags_list]

    group_index = []
    current_group = []
    for index in range(2, len(data)):
        if len(group_index) == 0 and len(data[index]) > 0:
            current_group.append(data[index])
            continue
        if len(data[index - 1]) == 0 and len(data[index - 2]) == 0:
            if len(current_group) > 0:
                group_index.append(current_group)
                current_group = []
            if len(data[index]) > 0:
                current_group.append(data[index])
            continue
        if len(data[index]) > 0:
            current_group.append(data[index])
    if len(current_group) > 0:
        group_index.append(current_group)
    return group_index

def get_href_to_premium(thumbs):

    href_to_prem = {}
    for thumb in thumbs:
        is_premium = "Объявление успешно оплачено" in thumb
        splitted = thumb.split(" ")
        for href in splitted:
            if "href" in href:
                link = href[6:-1]
                link = link.split("#")[0]
                break
        href_to_prem[link] = is_premium
    return  href_to_prem