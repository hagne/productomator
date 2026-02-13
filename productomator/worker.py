import pathlib as pl
import pandas as pd
import xarray as xr
import productomator.lab as prodlab


def files_between(root: pl.Path, start: pd.Timestamp, end: pd.Timestamp, globpattern: str = ""):
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
        year_dir = root / f"{d.year}"
        assert(year_dir.exists()), f'Year directory does not exist: {year_dir}'
        yield from year_dir.glob(f"*{d:%Y%m%d}{globpattern}")
        d += pd.to_timedelta(1, 'D')

class Workplanner():
    def __init__(self,
                 # data in and output folders
                 p2fld_in,
                 p2fld_out,
                 date_from_name,
                 output_file_format, #lalalal_{date}.nc'
                 glob_pattern_in = '*.nc',
                 start = None,
                 end = None,
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
        date_from_name : function
            A function that extracts a date from a filename. Example: lambda name: name.split('.')[-2].split('_')[-1]
        output_file_format : str
            A format string for naming output files, with a placeholder for the date, year, month, or day. You can define more placholders as long as you provide the variables in the kwargs or declare them in the subclass. 
            Example: '{site}_specflux_{date}.nc'
                In this example, provide the site variable in the kwargs or somehow declare self.site in the subclassing effort.
            Example 2: '{year}/monthly_product_{Year}{month}.nc'
                
        start: str or pd.Timestamp, optional
            Start date for processing. Have to provide end as well. glob_pattern_raw is still needed to define extension.
        end: str or pd.Timestamp, optional
            See start.
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


        self.output_file_format = output_file_format 
        
        p2fld_in = p2fld_in.format(**kwargs)
        self.p2fld_in = pl.Path(p2fld_in)
        if not self.p2fld_in.is_dir():
            raise ValueError(f'Input folder {self.p2fld_in} does not exist. Make sure the volume is mounted.')
        self.kwargs = kwargs
        for kw in kwargs:
            setattr(self, kw, kwargs[kw])

        p2fld_out = p2fld_out.format(**kwargs)
        self.p2fld_out = pl.Path(p2fld_out)
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

    def _get_input_files(self):
        if isinstance(self._processing_start, type(None)):
            if self.verbose:
                print(f'Get all files in {self.p2fld_in} with glob pattern: {self.glob_pattern_in}')
            gen = self.p2fld_in.glob(self.glob_pattern_in)
        else:
            start = pd.to_datetime(self._processing_start)
            end = pd.to_datetime(self._processing_end) if not isinstance(self._processing_end, type(None)) else pd.Timestamp.now()
            if self.verbose:
                print(f'Get all files in {self.p2fld_in} with "files_between" function and start: {start}, end: {end} and glob pattern: {self.glob_pattern_in}')
            gen = files_between(self.p2fld_in, start, end, globpattern = self.glob_pattern_in)
        df  = pd.DataFrame(gen, columns=['p2f_in'])
        return df

    def _make_master(self):
            df1 = self._get_input_files()
            df1.index = df1.apply(lambda row: pd.to_datetime(self.date_from_name(row.p2f_in.name)), axis = 1)
            df1.sort_index(inplace=True)
            mp = df1
            
            mp['p2f_out'] = mp.apply(lambda row: self.p2fld_out.joinpath(self.output_file_format.format(date = row.name.strftime("%Y%m%d"))), axis= 1)
            self._masterplan = mp
            return mp

    @property
    def masterplan(self):
        if self._masterplan is None:
            self._make_master()
        return self._masterplan



    @property
    def workplan(self):
        wp = self.masterplan.dropna()
        wp = wp[~(wp.apply(lambda row: row.p2f_out.is_file(), axis = 1))]
        return wp

    def process_row(self, row = None, iloc = None, loc = None):
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

        #######
        ## Open input files
        #######
        ds = xr.open_dataset(row.p2f_in)

        ## Do some processing here, e.g. add attributes, format the dataset, etc.

        ## Save the output file
        ds.to_netcdf(row.p2f_out)
        ds.close()
        return ds

    
    def process(self):
        for idx, row in self.workplan.iterrows():
            try:
                si = self.process_row(row)
                self.reporter.clean_increment()

            except:
                print('error rerun workplan to see what remained')
                self.reporter.errors_increment()
                continue
            
            print('.', end = '')


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
        
        df.index = df.p2f_in.apply(lambda p: pd.to_datetime(self.date_from_name(p.name)))
        df.sort_index(inplace=True)
        idx = df.index
        mp = pd.DataFrame(index= pd.date_range(idx[0].normalize(), idx[-1].normalize(), freq='D'), columns=['p2f_in', 'p2f_out'])
        # return df, wp
        if len(mp) == 0:
            self._masterplan = pd.DataFrame(columns=['p2f_in', 'p2f_out'])
            return self._masterplan
        
        mp['p2f_out'] = mp.apply(lambda row: self.p2fld_out / self.output_file_format.format(date = row.name.strftime("%Y%m%d"), 
                                                                                             year = row.name.strftime("%Y"),
                                                                                             month = row.name.strftime("%m"),
                                                                                             day = row.name.strftime("%d"),
                                                                                             **self.kwargs),
                                                                                             axis = 1)

        start_pos = df.index.searchsorted(mp.index, side='left') - 1
        start_pos = [int(sp) if sp>= 0 else 0 for sp in start_pos]
        end_pos = end_pos = df.index.searchsorted(mp.index + pd.Timedelta(days=1), side='left')
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
            dst = xr.open_dataset(last_row.p2f_out)
            self.tp_dst = dst.copy()
            try:
                dc = dst.day_complete.strip().lower()
                assert(dc in ['true','false']), f'day_complete needs to be True or False, found {dc}.'
                dc = dc == 'true'
                if not dc:
                    if self.verbose:
                        print(f'Output file {last_row.p2f_out} is not complete and will be re-processed.')
                    where.loc[last_idx] = False
            except:
                print(f'This is bound to happen. Implement error handling here.')
                raise 
            finally:
                dst.close()
        wp = wp[~where]
        return wp
    
    @workplan.setter
    def workplan(self, workplan):
        self._workplan = workplan
