#!/usr/bin/env python3

import pandas as pd
import os.path
from enum import Enum
from datetime import datetime
import warnings
from openpyxl import load_workbook
from tqdm import tqdm

# Filter out the UserWarning message
warnings.filterwarnings('ignore', category=UserWarning)

GroupBy = Enum('GROUP_BY', ['FILE_FUNCTION', 'FILE_FUNCTION_STUB', 'ALL_BUT_DATE', 'DONT_DROP_DUPLICATES'])
MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
PATH_TO_COMPILATION_ERRORS = '/mnt/c/Repos/Development/Metrics-Research/MetricResults/Kbuild Test Robot'
PATH_TO_BASELINES = '/mnt/c/Repos/Development/Metrics-Research/MetricResults/Baselines'
PATH_TO_DATASETS = '/mnt/c/Repos/Development/Metrics-Research/MetricResults/Datasets'

def printStatus(text):
    print('\033[1;34m' + text + '\033[0m')


def merge(df1, df2, mergeType = GroupBy.FILE_FUNCTION):
    """Merges two data frames and removes duplicates (keeps the first occurrence)
       MergeType specifies when 2 entries are treated as duplicate and should be removed:
         * FILE_FUNCTION: Removes all duplicates where file and function name are the same
         * FILE_FUNCTION_STUB: Tries to identify function stubs (at most 1 line of code) and wont remove an additional function implementation (more lines)
         * ALL_BUT_DATE: If there are two identical measures at different dates (e.g., 2 defect reports), it will keep both
         * DONT_DROP_DUPLICATES: Wont drop any duplicates
    """
    # Add auxillary columns if needed 
    if (mergeType == GroupBy.FILE_FUNCTION_STUB):
        df1['Stub'] = (df1['LoC'] <= 1)
        df2['Stub'] = (df2['LoC'] <= 1)

    # Perform merge
    result = pd.concat([df1, df2])
    
    # Filter duplicates
    # keep=first -> Keeps the first duplicate and drops all other occurrences
    if (mergeType == GroupBy.FILE_FUNCTION):
        result = result.drop_duplicates(['Source File', 'Element'], keep='first');
    if (mergeType == GroupBy.FILE_FUNCTION_STUB):
        result = result.drop_duplicates(['Source File', 'Element', 'Stub'], keep='first').drop(['Stub'], axis=1);
    if (mergeType == GroupBy.ALL_BUT_DATE):
        # Check if date column is named as 'Date' (error reports) or 'Date / Version' (merged training/evaluation dataset)
        if 'Date' in result.columns:
            result = result.drop_duplicates(subset=result.columns.difference(['Date']), keep='first')
        elif 'Date / Version' in result.columns:
            result = result.drop_duplicates(subset=result.columns.difference(['Date / Version']), keep='first')
        else:
            print('Error: Date column not found')
    if (mergeType == GroupBy.DONT_DROP_DUPLICATES):
        # Intended for debugging only, no action needed
        pass

    # Final result
    result = result.reset_index(drop=True)
    return result

def mergeErrorReports(baseFolder, years, mergeType = GroupBy.ALL_BUT_DATE, lastDate = None):
    result = None
    for y in years:
        for m in MONTHS:
            reportName = y + "-" + m
            folder = baseFolder + "/" + reportName
            if os.path.isdir(folder):
                df = pd.read_excel(baseFolder + "/" + reportName + "/" + reportName + ".xlsx", engine="openpyxl")
                if result is None:
                    result = df
                else:
                    result = merge(result, df, mergeType)
            else:
                print(folder + " doesn't exist")

    if lastDate is not None:
        date = datetime.strptime(lastDate, '%d.%m.%Y').date()
        print('Drop all reports after: ' + str(date))
        result['DateObject'] = pd.to_datetime(result['Date'], format='%a, %d %b %Y %H:%M:%S %z')
        result = result[result['DateObject'].dt.date <= date]
        result = result.drop('DateObject', axis=1)

    return result;

def mergeDefectReports(years, mergeType = GroupBy.ALL_BUT_DATE, lastDate = None):
    result = mergeErrorReports(PATH_TO_COMPILATION_ERRORS, years, mergeType, lastDate)
    destName = years[0]
    if lastDate is not None:
        destName += "-" + lastDate
    elif len(years) > 1:
        destName += "-" + years[-1]
    
    destName += ".csv"
    result.to_csv(PATH_TO_COMPILATION_ERRORS + "/" + destName, index=False, sep=';')

def createDefectReportFile():
    mergeDefectReports(['2013'], mergeType = GroupBy.ALL_BUT_DATE, lastDate='13.12.2013')

def testMerge():
    df0 = pd.read_csv('report0.csv', sep=';')
    df1 = pd.read_csv('report1.csv', sep=';')
    merged = merge(df0, df1, GroupBy.ALL_BUT_DATE)

    print(merged)

def loadBaseline(baseline):
    path = PATH_TO_BASELINES + '/linux-' + baseline + '/linux-' + baseline + '.csv'
    df_baseline = pd.read_csv(path, sep=';')
    df_baseline.insert(0, 'Date / Version', baseline)
    df_baseline.insert(3, 'Error', 0)

    return df_baseline;

def loadDefects(years, mergeType = GroupBy.ALL_BUT_DATE, lastDate = None):
    defects = mergeErrorReports(PATH_TO_COMPILATION_ERRORS, years, mergeType, lastDate)
    defects.rename(columns={'Date':'Date / Version'}, inplace=True)
    defects.insert(3, 'Error', 1)

    return defects

def filterByDiff(df, pathToDiff, modifiedFlags = None):
    diff = pd.read_csv(pathToDiff, sep=';');
    df['Edited'] = False
    nRowsBefore = len(df)

    if modifiedFlags is not None:
        # Drop all rows that are not modified by the given flag
        diff = diff[diff['Modified'].isin(modifiedFlags)]

    # Drop all files that where not edited
    for index, row in tqdm(diff.iterrows(), total=df.shape[0]):
        file = row['File']
        # If File ends with .c or .h its a source file, otherwise it could be a folder
        if file.endswith('.c') or file.endswith('.h'):
            # Mark file as edited in df['Edited']
            df.loc[df['Source File'] == file, 'Edited'] = True            
        else:
            # Mark all files in df as edited that are in the same folder
            df.loc[df['Source File'].str.startswith(file), 'Edited'] = True
    
    # Drop all rows that are not edited
    df = df[df['Edited'] == True]

    # Drop auxiliary column
    df = df.drop('Edited', axis=1)
    nRowsAfter = len(df)

    # Print how many rows were dropped
    print('Dropped ' + str(nRowsBefore - len(nRowsAfter)) + ' rows')
    return df

def createTrainingData(years, lastDate, baseline, pathToDiff, modifiedFlags):
    printStatus('Create training data for years: ' + str(years) + ' until ' + lastDate + ' with baseline ' + baseline)
    printStatus('Load defects')
    defects = loadDefects(years, mergeType = GroupBy.ALL_BUT_DATE, lastDate=lastDate)
    defects.drop(['Repository', 'Commit', 'Type'], axis=1, inplace=True)
    printStatus('Load baseline')
    df_baseline = loadBaseline(baseline)
    printStatus('Merge')
    merged = merge(defects, df_baseline, GroupBy.FILE_FUNCTION_STUB)
    printStatus('Filter by diff')
    merged = filterByDiff(merged, pathToDiff, modifiedFlags)
    printStatus('Sort')
    # Sort by file and function name
    merged = merged.sort_values(by=['Source File', 'Element'])
    printStatus('Save dataset')
    # Keep only first 5 columns
    # merged = merged.iloc[:, :5]
    merged.to_csv(PATH_TO_DATASETS + "/trainingData.csv", index=False, sep=';')
    printStatus('Done')


createTrainingData(years=['2013'], lastDate='03.11.2013', baseline='3.12', pathToDiff='/home/elscha/Linux-Experiment/simplifiedDiff.csv', modifiedFlags=None)
# test()