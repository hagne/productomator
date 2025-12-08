import pathlib as pl
import pandas as pd

class Workplanner():
    def __init__(self,
                 # data in and output folders
                 p2fld_in,
                 p2fld_out,
                 date_from_name,
                 output_file_format, #lalalal_{date}.nc'
                 **kwargs,
                ):
        """
        date_from_name = lambda name: name.split('.')[-2].split('_')[-1]
        output_file_format = f'{site}_specflux_{{date}}.nc'
        """
        self.output_file_format = output_file_format 
        self.p2fld_in = pl.Path(p2fld_in)
        self.p2fld_out = pl.Path(p2fld_out)
        self.date_from_name = date_from_name
        
        self._masterplan = None    

        

    @property
    def masterplan(self):
        if self._masterplan is None:
            self.p2fld_out
            self.p2fld_out.mkdir(parents=True, exist_ok=True)
            
            # all available MFRSR files
            df1 = pd.DataFrame(self.p2fld_in.glob('*'), columns= ['p2f_in',])
            df1.index = df1.apply(lambda row: pd.to_datetime(self.date_from_name(row.p2f_in.name)), axis = 1)
            df1.sort_index(inplace=True)
            
            # combine the two
            mp = df1 #pd.concat([df1, df2], axis = 1)
            
            mp['p2f_out'] = mp.apply(lambda row: self.p2fld_out.joinpath(self.output_file_format.format(date = row.name.strftime("%Y%m%d"))), axis= 1)
            
            self._masterplan = mp
        return self._masterplan



    @property
    def workplan(self):
        wp = self.masterplan.dropna()
        wp = wp[~(wp.apply(lambda row: row.p2f_out.is_file(), axis = 1))]
        return wp

    def process_row(self, row = None, iloc = None, loc = None):
        if iloc is not None:
            row = self.workplan.iloc[iloc]
        elif loc is not None:
            row = self.workplan.loc[loc]
        self.tp_row = row

        assert(False), 'You will want to overwrite this methode with somthing tailored to your needs!'
    
    def process(self):
        for idx, row in self.workplan.iterrows():
            try:
                si = self.process_row(row)
            except:
                print('error rerun workplan to see what remained')
                continue
                
            si.dataset.to_netcdf(row.p2f_out)
            print('.', end = '')