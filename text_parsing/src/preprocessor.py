from nltk.corpus import stopwords


class Preprocessor(object):

    @staticmethod
    def strip_lines(line: str):
        line = line.strip()
        line = ' '.join(line.split())
        return line

    @staticmethod
    def contract_lines(line: str):
        contraction_dict = {"ain't": "are not", "'s": " is", "aren't": "are not", "don't": "do not",
                            "didn't": "did not", "won't": "will not",
                            "can't": "cannot"}

        words = line.split()
        for i in range(len(words)):
            if words[i] in contraction_dict:
                words[i] = contraction_dict[words[i]]
        return ' '.join(words)

    @staticmethod
    def to_lower_case(line: str):
        return line.lower()

    @staticmethod
    def remove_punctuations(line: str):
        import string

        line = line.translate(str.maketrans('', '', string.punctuation))
        return line

    @staticmethod
    def remove_stopwords(line: str):
        stop_words = set(stopwords.words('english'))
        return " ".join([word for word in line.split() if word not in stop_words])

    @staticmethod
    def remove_special_chars(line: str):
        import re
        line = re.sub('[-+.^:,]', '', line)
        return line

    @staticmethod
    def remove_emojis(line: str):
        import re

        emoj = re.compile("["
                          u"\U0001F600-\U0001F64F"  # emoticons
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                          u"\U00002500-\U00002BEF"  # chinese char
                          u"\U00002702-\U000027B0"
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U0001f926-\U0001f937"
                          u"\U00010000-\U0010ffff"
                          u"\u2640-\u2642"
                          u"\u2600-\u2B55"
                          u"\u200d"
                          u"\u23cf"
                          u"\u23e9"
                          u"\u231a"
                          u"\ufe0f"  # dingbats
                          u"\u3030"
                          "]+", re.UNICODE)
        return re.sub(emoj, '', line)

    @staticmethod
    def split_data(line: str):
        line = line.split(maxsplit=1)
        if len(line) == 2:
            value = line[1]
            label = 1 if line[0] == 'label1' else 0
            return [value, label]
