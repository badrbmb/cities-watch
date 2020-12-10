import langid
import requests
import pandas as pd

from cities_watch import config


def request_wikimedia(name, language='en', start='20100101', end='20200101', granularity='monthly',
                      ref_url=config.REF_WIKI_URL):
    url = ref_url.format(language=language, name=name, start=start, end=end, granularity=granularity)
    r = requests.get(url=url)
    return r.json()


def get_city_pageviews(name, alt_names=None, ref_wiki=None, language='en', start='20100101', end='20200101',
                       granularity='monthly', verbose=config.VERBOSE, count=0):
    if (ref_wiki == ref_wiki) and ref_wiki:
        # When Wikipedia reference is available, to be used primarily
        try:
            ll, nn = ref_wiki.split(':')
            data = request_wikimedia(name=nn, language=ll, start=start, end=end, granularity=granularity)
            return pd.DataFrame(data['items'])['views'].sum()
        except Exception as e:
            pass

    if name != name:
        # if name is nan return 0
        return 0

    try:
        data = request_wikimedia(name=name, language=language, start=start, end=end, granularity=granularity)
        return pd.DataFrame(data['items'])['views'].sum()
    except Exception as e:
        # try in locale language:
        if count == 0:
            lg_alpha_2 = langid.classify(name)[0]
            if verbose:
                print(f'Trying in local language: {lg_alpha_2} ...')
            res = get_city_pageviews(name, alt_names=alt_names, language=lg_alpha_2, start=start, end=end,
                                     granularity=granularity, verbose=verbose, count=count + 1)
            return res
        else:
            # try alternative names
            if (alt_names == alt_names) and alt_names:
                list_names = [t.strip() for t in alt_names.split(';')]
                for ll in list_names:
                    res = get_city_pageviews(ll, alt_names=None, start=start, end=end,
                                             granularity=granularity, verbose=verbose, count=0)
                    if res != 0:
                        return res
                return 0
            else:
                if verbose:
                    print(f'No match found for: {name}')
                return 0
    except Exception as e:
        print(e)
        return 0
