import pathlib as pl
import socket
import sqlite3
import pandas as pd
import xarray as xr
import productomator.lab as prodlab


def files_between(root: pl.Path, start: pd.Timestamp, end: pd.Timestamp, globpattern: str = "", input_directory_structure: str = "yearly"):
    """ Generator that yields all files between start and end dates (inclusive) in the given root directory.
    Parameters
    ----------
    root : pl.Path
        Root directory containing year subdirectories with files.
    start : pd.Timestamp
        Start date.
    end : pd.Timestamp
        End date.
    globpattern : str, optional
        Glob pattern to match files. The default is mainly covering the extension, set to '*.nc' for netcdf files."".
    Yields
    -------
    pl.Path
        Paths to files between start and end dates.
    """ 
    assert(end > start), f'End must come after start! (end: {end}, start{start})'
    root = root
    d = start
    while d <= end:
        if input_directory_structure == 'yearly':
            year_dir = root / f"{d.year}"
        else:
            year_dir = root
        yield from year_dir.glob(f"*{d:%Y%m%d}{globpattern}")
        d += pd.to_timedelta(1, 'D')

class Workplanner():
    def __init__(self,
                 # data in and output folders
                 p2fld_in,
                 p2fld_out = None,
                 database = None,
                 date_from_name = None,
                 output_file_format = None, #lalalal_{date}.nc'
                 glob_pattern_in = '*.nc',
                 start = None,
                 end = None,
                 input_directory_structure = 'flat',
                 output_directory_structure = None,
                 file_complete_check = False, # only allows processing of files that are complete
                 reporter = None,
                 verbose = False,
                 **kwargs,
                ):
        """
        A generic workplanner class that can be used to plan and execute data processing tasks.
        Parameters
        ----------
        p2fld_in : str or pathlib.Path
            Path to the input folder containing data files to be processed.
        p2fld_out : str or pathlib.Path
            Path to the output folder where processed files will be saved.
        database : tuple, optional
            Currently, only database or p2fld_out can be set. When database is set, the dates in the masterplan and the dates column in the database will be mached. Missing values will be left for the workplan.
            Database is a tuple of 
                1. Path to a database 
                2. Name of the table in the database
                3. Column name of the date
                4. Column name of the file path; If "None", the date will be used to detemine if a file needs processing.
            Example: 
                ('/path/to/database.sqlite', 'table_name', 'row_timestamp', 'input_file'), his will match the file names in the p2fld_in with the input_file in the database table.
                ('/path/to/database.sqlite', 'table_name', 'row_timestamp', 'None'), this will match the dates in the masterplan (derived from the p2fld_in) with the dates in the database table (in the row_timestamp column).
        date_from_name : function
            A function that extracts a date from a filename. Not from the entire path, just the name (str)!! Example: lambda name: name.split('.')[-2].split('_')[-1]
        output_file_format : str
            A format string for naming output files, with a placeholder for the date, year, month, or day. You can define more placholders as long as you provide the variables in the kwargs or declare them in the subclass. 
            Example: '{site}_specflux_{date}.nc'
                In this example, provide the site variable in the kwargs or somehow declare self.site in the subclassing effort.
            Example 2: '{year}/monthly_product_{Year}{month}.nc'
                
        start: str or pd.Timestamp, optional
            Start date for processing. Have to provide end as well. glob_pattern_raw is still needed to define extension.
        end: str or pd.Timestamp, optional
            See start.
        input_directory_structure: str, optional
            Define the input directory structure, either 'yearly' or 'flat'.
                yearly: expects subdirectories for each year, e.g. /data/2020/, /data/2021/, etc.
                flat: expects all files in the root input directory.
        output_directory_structure: str, optional
            Currently only None is allowed, which means that the output directory structure is the same as the input directory structure.
        file_complete_check: bool, optional
            If True, the workplanner will check if the existing output files are complete by looking for a day_complete attribute in the file. 
            If the attribute is False, the file will be re-processed. If the first complete file is found, the attribute will no longer be checked for older files, as they are assumed to be complete as well. 
        glob_pattern : str, optional
            A glob pattern to match input files. Default is '*.nc'.

        Examples
        --------
        You will likely want to subclass this class and overwrite the process_row method, for example:

        class CalibrateMFR(pm.worker.Workplanner):
            def __init__(self,instrument,*args, **kwargs):
                super().__init__(*args, **kwargs)
                self.instrument = instrument
                
            def process_row(self, row = None, iloc = None, loc = None):
                if iloc is not None:
                    row = self.workplan.iloc[iloc]
                elif loc is not None:
                    row = self.workplan.loc[loc]
                self.tp_row = row
                ds = self.instrument.raw2calibrated(row.p2f_in)
                return ds
        """

        self.file_complete_check = file_complete_check
        self.output_file_format = output_file_format 
        self.input_directory_structure = input_directory_structure
        if not isinstance(input_directory_structure, str):
            raise TypeError(f'input_directory_structure must be a string, got {type(input_directory_structure)}')
        if input_directory_structure not in ['yearly', 'flat']:
            raise ValueError(f'input_directory_structure must be either "yearly" or "flat", got {input_directory_structure}')
        assert(output_directory_structure in [None]), 'Currently only None is allowed, which means that the output directory structure is the same as the input directory structure. Programming required for different output directory structure.'
        if output_directory_structure is None:
            output_directory_structure = input_directory_structure
        self.output_directory_structure = output_directory_structure

        p2fld_in = p2fld_in.format(**kwargs)
        self.p2fld_in = pl.Path(p2fld_in)
        if not self.p2fld_in.is_dir():
            raise ValueError(f'Input folder {self.p2fld_in} does not exist. Make sure the volume is mounted.')
        self.kwargs = kwargs
        for kw in kwargs:
            setattr(self, kw, kwargs[kw])


        #####
        # the output folder or database
        self.database = database
        self.p2fld_out = p2fld_out
        if not isinstance(database, type(None)) and not isinstance(p2fld_out, type(None)):
            raise ValueError('Currently, only one of database or p2fld_out can be set.')

        if database is not None:
            self.database = database
        else:
            p2fld_out = p2fld_out.format(**kwargs)
            self.p2fld_out = pl.Path(p2fld_out)
            if output_directory_structure == 'yearly':
                self.p2fld_out = self.p2fld_out / '{year}'

        self.date_from_name = date_from_name
        self.glob_pattern_in = glob_pattern_in
        if isinstance(reporter, type(None)):
            self.reporter = prodlab.Reporter()
        elif isinstance(reporter, prodlab.Reporter):
            self.reporter = reporter
        else:
            raise TypeError(f'reporter must be a prodlab.Reporter or None, got {type(reporter)}')
        self.verbose = verbose

        self._processing_start = start
        self._processing_end = end

        self._masterplan = None   

    def _read_database(self):
        # read the datacolumn from the database
        def connect(path2database):
            conn = sqlite3.connect(path2database)
            conn.row_factory = sqlite3.Row
            return conn
        path2database, table_name, date_column, p2f_column = self.database
        with connect(path2database) as conn:
            df = pd.read_sql_query(
                f'SELECT * FROM {table_name}',# ORDER BY {date_column} DESC',
                conn,
                # index_col='row_timestamp',
            )
        if len(df) == 0:
            print(f'Warning: Database {path2database} table {table_name} is empty.')
            return None
        if p2f_column == 'None':
            df['in_database'] = True
        else:
            df['in_database'] = df.apply(lambda row: pl.Path(row[p2f_column]).name, axis = 1)
        df.index  = df.apply(lambda row: pd.to_datetime(row[date_column]), axis = 1)
        df.sort_index(inplace=True)
        return df


    def _get_input_files(self):
        if isinstance(self._processing_start, type(None)):
            if self.verbose:
                print(f'Get all files in {self.p2fld_in} with glob pattern: {self.glob_pattern_in}')
            if self.input_directory_structure == 'yearly':
                gen = self.p2fld_in.glob(f"*/{self.glob_pattern_in}")
            else:
                gen = self.p2fld_in.glob(self.glob_pattern_in)
        else:
            start = pd.to_datetime(self._processing_start)
            end = pd.to_datetime(self._processing_end) if not isinstance(self._processing_end, type(None)) else pd.Timestamp.now()
            if self.verbose:
                print(f'Get all files in {self.p2fld_in} with "files_between" function and start: {start}, end: {end} and glob pattern: {self.glob_pattern_in}')
            gen = files_between(self.p2fld_in, start, end, globpattern = self.glob_pattern_in, input_directory_structure = self.input_directory_structure)
        df  = pd.DataFrame(gen, columns=['p2f_in'])
        return df

    def _make_master(self):
            df1 = self._get_input_files()
            df1.index = df1.apply(lambda row: pd.to_datetime(self.date_from_name(row.p2f_in.name)), axis = 1)
            df1.sort_index(inplace=True)
            mp = df1      
            if self.p2fld_out is not None:          
                mp['p2f_out'] = mp.apply(lambda row: pl.Path(str(self.p2fld_out.joinpath(self.output_file_format)).format(date = row.name.strftime("%Y%m%d"), 
                                                                                                                        year = row.name.strftime("%Y"),
                                                                                                                        month = row.name.strftime("%m"),
                                                                                                                        day = row.name.strftime("%d"),
                                                                                                                        **self.kwargs)),
                                                                                                                    axis= 1) # this might look overly complicated but is necessary to do the full formatting including yearly subdirectories if needed.
            elif self.database is not None:
                df = self._read_database()
                if df is None:
                    mp['in_database'] = None
                else:
                    mp['in_database'] = df.in_database
            else:
                raise ValueError('Either p2fld_out or database must be set.')    
            assert(mp.index.is_monotonic_increasing), 'Masterplan index is not monotonic increasing, check the date parsing from the file names.'
            self._masterplan = mp
            return mp

    def combine_masterplan_duplicates(self):
        """Combine master-plan rows that share the same timestamp."""
        if isinstance(self._masterplan, type(None)):
            self._make_master()
        mp = self._masterplan
        if mp.index.is_unique:
            if self.verbose:
                print('Masterplan index is already unique, no need to combine duplicates.')
            return 
        grouped = mp.groupby(level=0, sort=False)
        if self.p2fld_out is not None:
            combined = pd.DataFrame({
                'p2f_in': grouped['p2f_in'].agg(lambda s: s.iloc[0] if len(s) == 1 else list(s)),
                'p2f_out': grouped['p2f_out'].first(),
            })
        elif self.database is not None:
            combined = grouped.agg({
                'p2f_in': lambda s: s.iloc[0] if len(s) == 1 else list(s),
                'in_database': 'first',
            })
        else:
            raise ValueError('Either p2fld_out or database must be set.')
        self._masterplan = combined.sort_index()
        return self._masterplan

    @property
    def masterplan(self):
        """
        Troubleshooting tips
        ----------------------
        If the masterplan is empty:
        1. Check if the input directory is correct and mounted. 
            - try: self.p2fld_in.exists()
            - try: self._get_input_files() """
        if self._masterplan is None:
            self._make_master()
            mp = self._masterplan
            assert(mp.index.is_unique), 'Masterplan index is not unique. Consider running the combine_masterplan_duplicates or use the WorkplannerDaily class, which allows truncating files that contribute to multiple days to daily files.'
        return self._masterplan

    @property
    def workplan(self):
        if self.p2fld_out is not None:
            # Files that don't exist must be processed.
            wp = self.masterplan.dropna()
            exists = wp.p2f_out.apply(lambda p: p.is_file())
            where_reprocess = ~exists

            # If disabled, don't open any files.
            if not self.file_complete_check:
                return wp[where_reprocess]

            # Check only trailing existing files (newest -> oldest) until first complete day.
            for idx, row in wp[exists].iloc[::-1].iterrows():
                with xr.open_dataset(row.p2f_out) as ds:
                    assert hasattr(ds, "day_complete"), (
                    f"Input files need a day_complete attribute for file-complete checks. Missing in {row.p2f_out}"
                    )
                    dc = ds.day_complete
                    complete = dc.strip().lower() == "true"

                if complete:
                    break  # older files are assumed already complete
                else:
                    where_reprocess.loc[idx] = True  # reprocess incomplete trailing file(s)
            return wp[where_reprocess]

        elif self.database is not None:
            mp = self.masterplan 
            path2database, table_name, date_column, p2f_column = self.database
            if p2f_column == 'None':
                in_db = ~self.masterplan.in_database.isna()
            else:
                in_db = mp.apply(lambda row: row.p2f_in.name == row.in_database, axis = 1)
            wp = mp[~in_db]
            wp = wp.drop('in_database', axis = 1)
            return wp
        else:
            raise ValueError("Either p2fld_out or database must be set.")
        return 

    # @property
    # def workplan(self):
    #     wp = self.masterplan.dropna()
    #     file_complete_check = self.file_complete_check
    #     def check_file_exists_and_complete(row):
    #         if not row.p2f_out.is_file():
    #             return False
    #         else:
    #             if file_complete_check:
    #                 with xr.open_dataset(row.p2f_out) as ds:
    #                     assert(hasattr(ds, 'day_complete')), 'Input files need to have a day_complete attribute for the file complete check.'
    #                     complete = bool(ds.day_complete)
    #                 if complete:
    #                     file_complete_check = False # only check until the first comple file is found.
    #                 return complete
    #             #test if complete
    #     # wp = wp[~(wp.apply(lambda row: row.p2f_out.is_file(), axis = 1))]
    #     wp = wp[~(wp.apply(check_file_exists_and_complete, axis = 1))]
    #     return wp

    def process_row(self, row = None, iloc = None, loc = None, save = True):
        """This is the method that does the particular work and will need to be overwritten in your subclass.
        Typical components:
        1. read the input file(s) (row.p2f_in)
        3. convert to xarray dataset (if needed)
        2. format the netcdf file
            2.1 add dataset attributes, creation datetime, creation software, server, site details, etc.
            2.2 add variable attributes, units, long_name, standard_name, etc.
        3. save the output file (row.p2f_out)
        
        Parameters
        ----------
        row : pandas.Series, optional
            A row from the workplan dataframe. This is how the process method callse this function.
        iloc : int, optional
            An integer index to select a row from the workplan dataframe.
        loc : index label, optional
            select a row by timestamp.
            """
        
        if iloc is not None:
            row = self.workplan.iloc[iloc]
        elif loc is not None:
            row = self.workplan.loc[loc]
        self.tp_row = row

        #####
        # get last processed instance - usefull if processing depends on the previous day
        ######
        lastrow = self.get_last_row_before_workplan()
        if isinstance(lastrow, type(None)):
            assert(False), 'set defaults?'
        dslast = xr.open_dataset(lastrow.p2f_out)

        #######
        ## Open input files
        #######
        if isinstance(row.p2f_in, list):
            ds = xr.open_mfdataset(row.p2f_in)
            input_files = ','.join(str(p) for p in row.p2f_in)
        else:
            ds = xr.open_dataset(row.p2f_in)
            input_files = str(row.p2f_in)

        ## Do some processing here, e.g. add attributes, format the dataset, etc.
        #####
        # Format the dataset variables, this includes reordering and dropping variables.
        reorg = ['','','','','',]

        ds = ds[reorg]

        #########
        # Format the dataset attributes
        #########
        dropattrs = [
                    # '','','','','',
                    ]
        for a in dropattrs:
            ds.attrs.pop(a)

        ds.attrs['parent_files'] = row.p2f_in.as_posix()
        ds.attrs['processing_date'] = pd.Timestamp.now().isoformat()
        ds.attrs['processing_server'] = socket.gethostname()
        ds.attrs['processing_class'] = f"This file was generated using{self.__class__.__module__}.{self.__class__.__qualname__}"
        ds.attrs['product_version'] = self.version
        ## Save the output file
        if save:
            ds.to_netcdf(row.p2f_out)
        ds.close()
        return ds

    
    def process(self, raise_errors = False):
        for idx, row in self.workplan.iterrows():
            try:
                si = self.process_row(row)
                self.reporter.clean_increment()

            except Exception as e:
                if raise_errors:
                    raise e
                else:
                    print(f'Error occurred while processing row {idx}: {e}')
                    self.reporter.errors_increment()
                    continue
            
            print('.', end = '')

    def get_last_row_before_workplan(self):
        try:
            idx = self.workplan.iloc[0].name
        except IndexError:
            print('workplan is empty')
            return None
        loc = self.masterplan.index.get_loc(idx)
        if isinstance(loc, slice):
            loc = loc.start
            assert(False), "The workplan index contains duplicates, this is currently not supported in Workplanner. Consider useing WorkplannerDaily or adjust"
        loc -=  1
        if loc < 0:
            print('Masterplan and Workplan are identical. Either this is the first time the script is run with this configuration or the start data needs to be adjusted')
            return None
        return self.masterplan.iloc[loc]


class WorkplannerDaily(Workplanner):
    def _make_master(self):
        """This function tries to find all input files that can potendioally contribute to the output files of each day.
        This is done by looking at the last day befor and first day after the day in question.
        """

        # df = pd.DataFrame({'p2f_in': paths})
        df = self._get_input_files()
        if df.empty:
            self._masterplan = pd.DataFrame(columns=['p2f_in', 'p2f_out'])
            return self._masterplan
        try:
            file_names = [p.name for p in df.p2f_in.to_numpy()]
            dates = [self.date_from_name(name) for name in file_names]
            df.index = pd.to_datetime(dates)
        except (ValueError, TypeError, IndexError) as e:
            raise ValueError(f"Error parsing dates from file names, e.g {file_names[0]}. Make sure the Workplanner's date_from_name function is defined and correct. Original error: {e}") from e
        
        df.sort_index(inplace=True)
        idx = df.index
        mp = pd.DataFrame(index= pd.date_range(idx[0].normalize(), idx[-1].normalize(), freq='D'), columns=['p2f_in', 'p2f_out'])
        # return df, wp
        if len(mp) == 0:
            self._masterplan = pd.DataFrame(columns=['p2f_in', 'p2f_out'])
            return self._masterplan
        
        mp['p2f_out'] = mp.apply(lambda row: pl.Path(str(self.p2fld_out / self.output_file_format).format(date = row.name.strftime("%Y%m%d"), 
                                                                                             year = row.name.strftime("%Y"),
                                                                                             month = row.name.strftime("%m"),
                                                                                             day = row.name.strftime("%d"),
                                                                                             **self.kwargs)),
                                                                                             axis = 1)

        start_pos = df.index.searchsorted(mp.index, side='left') - 1
        start_pos = [int(sp) if sp>= 0 else 0 for sp in start_pos]
        end_pos = df.index.searchsorted(mp.index + pd.Timedelta(days=1), side='left')
        idxmax = len(df)-1
        end_pos =  [int(sp) if sp<= idxmax else idxmax for sp in end_pos]

        self.tp_start_pos = start_pos
        self.tp_end_pos = end_pos
        self.tp_df = df
        mp['p2f_in'] = [list(df.p2f_in.iloc[s:e + 1]) for s, e in zip(start_pos, end_pos)]

        # check if an output file will be complete. It is complete if it contains an input file from the following day.
        mp['day_complete'] = [list(df.iloc[s:e + 1].index) for s, e in zip(start_pos, end_pos)]
        mp['day_complete'] = mp.apply(lambda row: row.name.normalize() < row.day_complete[-1].normalize(), axis = 1) #true if the last input file is at least from the following day.
        self._masterplan = mp
    
    @property
    def workplan(self):
        wp = self.masterplan.dropna()
        where = wp.apply(lambda row: row.p2f_out.is_file(), axis = 1)
        self.tp_where = where.copy()
        if where.any():
            last_idx = where[where].index[-1]
            last_row = wp.loc[last_idx]
            with xr.open_dataset(last_row.p2f_out) as dst:
                dst = xr.open_dataset(last_row.p2f_out)
                self.tp_dst = dst.copy()
                if 'day_complete' not in dst.attrs:
                    raise AttributeError(f'File complete check is enabled, but the day_complete attribute is missing in last processed file {last_row.p2f_out}. Add "attrs["day_complete"] = row.day_complete.__str__()" to the process_row method of your Workplanner subclass to fix this. You will also need to remove the previous file or somehow add the attribute to it.')
                dc = dst.day_complete.strip().lower()
                assert(dc in ['true','false']), f'day_complete needs to be True or False, found {dc}.'
                dc = dc == 'true'
            if not dc:
                if self.verbose:
                    print(f'Output file {last_row.p2f_out} is not complete and will be re-processed.')
                where.loc[last_idx] = False

        wp = wp[~where]
        return wp
    
    @workplan.setter
    def workplan(self, workplan):
        self._workplan = workplan
