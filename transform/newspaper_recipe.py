import argparse
import logging
import hashlib
import nltk
from nltk.corpus import stopwords
from urllib.parse import urlparse
import pandas as pd
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

nltk.download('punkt')
nltk.download('stopwords')


def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspapaer_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_trailing_spaces(df)
    df = _remove_nan_values(df)
    df = _tokenize_column(df, 'title')
    df = _tokenize_column(df, 'body')
    df = _remove_duplicates_entries(df, 'title')
    _save_data(df, filename)
    return df


def _save_data(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Saving data at location: {}'.format(clean_filename))
    df.to_csv(clean_filename)


def _remove_duplicates_entries(df, column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df


def _tokenize_column(df, column):
    logger.info('Tokenizing Column {}'.format(column))
    stop_words = set(stopwords.words('spanish'))

    n_tokens = (
        df
        .dropna()
        .apply(lambda row: nltk.word_tokenize(row[column]), axis=1)
        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
        .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
        .apply(lambda valid_word_list: len(valid_word_list))
    )

    df['n_tokens_n' + column] = n_tokens
    return df


def _remove_nan_values(df):
    logger.info('Removing nan values')
    return df.dropna()


def _remove_trailing_spaces(df):
    logger.info('Removing trailing spaces')
    df['body'] = df['body'].str.strip()
    return df


def _generate_uids_for_rows(df):
    logger.info('Filling uids for rows')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    return df.set_index('uid')


def _fill_missing_titles(df):
    logger.info('Filling missing title')
    missing_titles_mask = df['title'].isna()

    missing_titles = (df[missing_titles_mask]['url']
                      .str.extract(r'(?P<missing_titles>[^/]+)$')
                      .applymap(lambda title: title.split('-'))
                      .applymap(lambda title_word_list: ' '.join(title_word_list))
                      )

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df


def _extract_host(df):
    logger.info('Extracting host from url')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df


def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper uid column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid
    return df


def _extract_newspapaer_uid(filename):
    logger.info('Extracting newspaper uid')
    return filename.split('_')[0]


def _read_data(filename):
    logger.info('Reading File {}'.format(filename))
    return pd.read_csv(filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='Path to csv data', type=str)
    args = parser.parse_args()
    dataframe = main(args.filename)
    print(dataframe)
