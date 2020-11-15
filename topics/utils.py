import os
import sys
import numpy as np

def load_cached_uids(year: int, month: int) -> np.array:
    month_mapper = {'1': 'jan',
                            '2': 'feb',
                            '3': 'mar',
                            '4': 'apr',
                            '5': 'may',
                            '6': 'jun',
                            '7': 'jul',
                            '8': 'aug',
                            '9': 'sep',
                            '10':'oct',
                            '11':'nov',
                            '12':'dec'}

    month = month_mapper[str(month)]

    file = os.path.join(os.path.expanduser('~'),os.path.join('topic_analysis',os.path.join(str(year), os.path.join(month, 'uid.npy'))))
    return np.load(file)


def cache_uids(year: int, month: int, data: np.array):
    if len(data) != 0:
        month_mapper = {'1': 'jan',
                                '2': 'feb',
                                '3': 'mar',
                                '4': 'apr',
                                '5': 'may',
                                '6': 'jun',
                                '7': 'jul',
                                '8': 'aug',
                                '9': 'sep',
                                '10':'oct',
                                '11':'nov',
                                '12':'dec'}

        month = month_mapper[str(month)]

        home = os.path.expanduser('~')

        folder = 'topic_analysis'

        directory = os.path.join(home, folder)

        if not os.path.isdir(directory):

            year_directory = os.path.join(directory, str(year))
            month_directory = os.path.join(year_directory, str(month))
            os.makedirs(month_directory, exist_ok=True)

            data_path = os.path.join(month_directory, 'uid.npy')

            np.save(data_path, data)
            return 

        else:

            year_directory = os.path.join(directory, str(year))

            if not os.path.isdir(year_directory):

                month_directory = os.path.join(year_directory, str(month))
                os.makedirs(month_directory, exist_ok=True)

                data_path = os.path.join(month_directory, 'uid.npy')

                np.save(data_path, data)
                return
            else:

                month_directory = os.path.join(year_directory, str(month))

                if not os.path.isdir(month_directory):
                    os.makedirs(month_directory, exist_ok=True)

                    data_path = os.path.join(month_directory, 'uid.npy')

                    np.save(data_path, data)
                    return

                else:
                    data_path = os.path.join(month_directory, 'uid.npy')
                    
                    np.save(data_path, data)
        



def check_for_cached_uid(year: int, month: int) -> bool:

    #get home dir
    home = os.path.expanduser('~')

    folder = 'topic_analysis'

    directory = os.path.join(home, folder)

    if not os.path.isdir(directory): return False

    else:
        year_directory = os.path.join(directory, str(year))

        if not os.path.isdir(year_directory): return False

        else:

            month_mapper = {'1': 'jan',
                            '2': 'feb',
                            '3': 'mar',
                            '4': 'apr',
                            '5': 'may',
                            '6': 'jun',
                            '7': 'jul',
                            '8': 'aug',
                            '9': 'sep',
                            '10':'oct',
                            '11':'nov',
                            '12':'dec'}

            month = month_mapper[str(month)]

            month_directory = os.path.join(year_directory, month)

            if not os.path.isdir(month_directory): return False

            else:
                uid_file = os.path.join(month_directory, 'uid.npy')

                if not os.path.isfile(uid_file): return False
                else: return True





if __name__ == '__main__':


    data = np.array([1,2,3,4,5])
    cache_uids(2020, 1, data)

    if check_for_cached_uid(2019,1):
        print(load_cached_uids(2020, 1))
    else:
        print('No data cached for this year and month')