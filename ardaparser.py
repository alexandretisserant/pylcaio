""" ArdaParser: Prepare foreground inventory LCA data

Parse the Arda template that contains the information for constructing 
the foreground model of a LCA.
Read in, prepare the data and build the matrices to make them readily
available to be used in pyLCAIO.
"""

import pandas as pd
import os

class ArdaParser:

    def __init__(self, path_to_template, parse = True):
        """
        Define an ArdaParser object that goes through the excel template
        defined by the path_to_template. By default, the template is fully
        parsed and matrices are built at the object instanciation.
        
        args:
        -----
        path_to_template: str
            path to the Arda template to be read
        parse: bool, default = True
            for convenience, tell whether or not to parse and build the
            matrices staight from the ArdaParser creation
        """

        # INITIALIZE ATTRIBUTES
        # input data
        self.path_to_template = os.path.abspath(path_to_template)
        self.foreground_sheet = pd.read_excel(self.path_to_template,
                                              sheet_name='Foreground',
                                              header=[2])
        self.foreground_sheet = remove_empty_rows_dataframe(self.foreground_sheet)
        self.A_bf_sheet = pd.read_excel(self.path_to_template,
                                              sheet_name='A_bf',
                                              header=[3])
        self.A_bf_sheet = remove_empty_rows_dataframe(self.A_bf_sheet)
        self.F_f_sheet = pd.read_excel(self.path_to_template,
                                              sheet_name='F_f',
                                              header=[3])
        self.F_f_sheet = remove_empty_rows_dataframe(self.F_f_sheet)

        # extended Labels
        self.PRO_f = pd.DataFrame()

        # main matrices, as Pandas Dataframes
        self.A_ff = pd.DataFrame()
        self.A_bf = pd.DataFrame()

        self.F_f = pd.DataFrame()

        self.y_f = pd.DataFrame()
        
        if parse:
            self.parse_template()
        
#-----------------------------------------------------------------------------#
    # PARSING METHODS        
        
    def read_PRO_f(self):
        """ Read the foreground processes extended metadata """
        pro_f = self.foreground_sheet.loc[:, 'FULL NAME':'UNIT']
        pro_f['PROCESS ID'] = pro_f['PROCESS ID'].apply(int)
        # pyLCAIO has its indexes as tuple...
        pro_f.index = pd.Index([(ind,) for ind
                                in pro_f['PROCESS ID'].values.tolist()])
        pro_f = pro_f.fillna('')
        self.PRO_f = pro_f
        
    def read_y_f(self):
        """ Read and build the foreground final demand y_f """
        y = self.foreground_sheet.loc[:, 'Unnamed: 10']
        y = y.fillna(0.0)
        y.index = self.PRO_f.index
        y.name = 'y'
        self.y_f = y
    
    def read_A_ff(self):
        """ Read and build the foreground to foreground matrix A_ff """
        a_ff = self.foreground_sheet.loc[:,
                                         self.PRO_f['FULL NAME'].values.tolist()]
        a_ff = a_ff.fillna(0.0)
        a_ff.index = self.PRO_f.index
        a_ff.columns = self.PRO_f.index
        self.A_ff = a_ff
        
    def read_A_bf(self):
        """ Read data for building the background to foreground matrix """
        flow_data = self.A_bf_sheet[['BACKGROUND ID', 'FOREGROUND ID','AMOUNT','Comment.2']]
        flow_data.columns = pd.Index(['BACKGROUND ID', 'FOREGROUND ID','AMOUNT','UNIT'])
        return flow_data
    
    def read_F_f(self):
        """ Read data for building the foreground stressors' matrix """
        flow_data = self.F_f_sheet[['STRESSOR ID','FOREGROUND ID','AMOUNT','Comment.2']]
        flow_data.columns = pd.Index(['STRESSOR ID', 'FOREGROUND ID','AMOUNT','UNIT'])
        return flow_data
        
    def parse_template(self):
        """ Call the different methods for reading/building the foregound matrices """
        self.read_PRO_f()
        self.read_y_f()
        self.read_A_ff()
        self.build_A_bf()
        self.build_F_f()
        
#-----------------------------------------------------------------------------#
    # MATRIX BUILDING METHODS 
    
    def build_A_bf(self):
        """
        Build the background to foreground matrix of flows.
        
        At the moment, the matrix is partially built. Only background and
        foreground processes that have non-null flows are included in the matrix.
        When passed to the LCA model builder/calculator (for instance pyLCAIO) 
        the rows and columns should be extended/resorted according to the full 
        A_bb and A_ff matrices (usually this can be taken care of with 
        pandas.concat())
        
        TODO: maybe, we should read the full list of fore an background
        processes, so that we build directly the full A_bf, it would allow also 
        to check for consistancy of units
        """
        flow_data = self.read_A_bf()
        index = pd.Index([(ind, ) for ind
                          in set(flow_data['BACKGROUND ID'].values.tolist())])
        a_bf = pd.DataFrame(index   = index,
                            columns = self.PRO_f.index).fillna(0.0)
        for i in flow_data.index:
            coeff = flow_data.loc[i, :]
            a_bf.loc[(coeff['BACKGROUND ID'], ), (coeff['FOREGROUND ID'], )] = coeff['AMOUNT']
        self.A_bf = a_bf
    
    def build_F_f(self):
        """
        Build the foreground emissions matrix of stressors.
        
        At the moment, the matrix is partially built. Only stressors and foreground 
        processes that have non-null stressor flows are included in the matrix.
        When passed to the LCA model builder/calculator (for instance pyLCAIO) 
        the rows and columns should be extended/resorted according to the full 
        F_b and A_ff matrices (usually this can be taken care of with 
        pandas.concat())
        
        TODO: maybe, we should read the full list of foreground process and stressors, 
        so that we build directly the full A_bf, it would allow also to check
        for consistancy of units
        """
        flow_data = self.read_F_f()
        index = [(stress,) for stress
                 in set(flow_data['STRESSOR ID'].values.tolist())]
        f_f = pd.DataFrame(index    = index,
                           columns  = self.PRO_f.index).fillna(0.0)
        for i in flow_data.index:
            coeff = flow_data.loc[i, :]
            f_f.loc[(coeff['STRESSOR ID'], ), (coeff['FOREGROUND ID'], )] = coeff['AMOUNT']
        self.F_f = f_f

#-----------------------------------------------------------------------------#
    # IO METHODS 
    
    def to_dict(self):
        
        pass

#-----------------------------------------------------------------------------#
    # SUPPORTING MODULE FUNCTIONS
    
def remove_empty_rows_dataframe(df):
    """ Remove empty excel rows from the dataframe """
    df = df.copy()
    rows_to_keep = []
    index = df.index.values.tolist()
    for i in index:
        if not df.loc[i, :].isnull().all(): # if they all non-null, we append
            rows_to_keep.append(i)
    return df.loc[rows_to_keep, :]

def remove_empty_cols_dataframe(df):
    """ Remove empty excel columns from the dataframe """
    df = df.copy()
    cols_to_keep = []
    index = df.columns.values.tolist()
    for i in index:
        if not df.loc[:, i].isnull().all(): # if they all non-null, we append
            cols_to_keep.append(i)
    return df.loc[:, cols_to_keep]