# Copyright (c) 2022, abayomi.awosusi@sgatechsolutions.com and contributors
# For license information, please see license.txt

import frappe
import calendar
import numpy as np
import json
#import openpyxl
import csv
import pandas as pd
from frappe import _, scrub
from frappe.utils import add_days, add_to_date, flt, getdate
from six import iteritems
from erpnext.accounts.utils import get_fiscal_year
#from openpyxl import load_workbook
from datetime import date

#
def execute(filters=None):
    return WeeklySales(filters).run()

#
class WeeklySales(object):
    def __init__(self, filters=None):
        self.filters = frappe._dict(filters or {})		
        self.date_field = (
            "transaction_date"
            # if self.filters.doc_type in ["Sales Order", "Purchase Order"]
            # else "posting_date"
        )
        self.months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]		
        self.get_period_date_ranges()
        #print("c1")

    def run(self):		
        #self.filters.tree_type="Cost Center"
        self.get_columns()
        self.get_data()
    
        # Skipping total row for tree-view reports
        skip_total_row = 0

    
        return self.columns, self.data, None, None, skip_total_row

    def get_columns(self):
        self.columns = [
            {
                "label": self.filters.cost_center,				
                "fieldname": "cost_center",
                "fieldtype": "data",
                "width": 140,
                "hidden":1
            }
        ]
        self.columns.append(
                {
                    "label": "Backlog", 
                    "fieldname": "Backlog", 
                    "fieldtype": "data",
                     "width": 120
                }
            )
        for end_date in self.periodic_daterange:
            period = self.get_period(end_date)					
            self.columns.append(
                {
                    "label": _(period), 
                    "fieldname": scrub(period), 
                    "fieldtype": "Float",
                     "width": 120
                }
            )
        self.columns.append(
            {"label": _("Total"), "fieldname": "total", "fieldtype": "Float", "width": 120}
        )

    def get_data(self):				
        #if self.filters.tree_type == "Cost Center":			
        self.get_sales_transactions_based_on_cost_center()			
        self.get_rows()			

        value_field = "base_net_total as value_field"
        # if self.filters["value_quantity"] == "Value":
        # 	value_field = "base_net_total as value_field"
        # else:
        # 	value_field = "total_qty as value_field"

        entity = "project as entity"	
        # self.entries = frappe.get_all(
        # 	self.filters.cost_center,
        # 	fields=[entity, value_field, self.date_field],
        # 	filters={
        # 		"docstatus": 1,
        # 		"company": self.filters.cost_center,
        # 		"project": ["!=", ""],				
        # 	},
        # )
        
    def get_sales_transactions_based_on_cost_center(self):			
        value_field = "base_amount"		
        # self.entries = frappe.db.sql(
        # 	"""
        # 	(select distinct `tabSales Order`.grand_total,`tabSales Order Item`.item_group as entity,`tabSales Order`.cost_center, `tabSales Order`.name, 
        # 	`tabSales Order Item`.base_amount as value_field,`tabSales Order`.transaction_date,
        # 	`tabSales Order`.customer_name, `tabSales Order`.status,`tabSales Order`.delivery_status, 
        # 	`tabSales Order`.billing_status,`tabSales Order Item`.delivery_date from `tabSales Order`, 
        # 	`tabSales Order Item` where `tabSales Order`.name = `tabSales Order Item`.parent and 
        # 	`tabSales Order`.status <> 'Cancelled' and  `tabSales Order`.cost_center = %(cost_center)s
        # 	and `tabSales Order Item`.delivery_date <=  %(to_date)s
        # 	)	
        # """, {
        # 		'cost_center': self.filters.cost_center,'from_date': self.filters.start_date ,'to_date':  self.filters.to_date				
        # 	},		
        # 	as_dict=1,
        # )	
        #print("c5")
        #self.get_groups()		
        self.entries = frappe.db.sql(
        """
        (select distinct s.cost_center as entity, i.base_amount as value_field, s.transaction_date
         from `tabSales Order` s,`tabSales Order Item` i 
         where s.name = i.parent and 
        s.status <> 'Cancelled' and  s.cost_center = %(cost_center)s
        and i.delivery_date <=  %(to_date)s
        )	
        """, {
                'cost_center': self.filters.cost_center,'from_date': self.filters.start_date ,'to_date':  self.filters.to_date				
            },		
            as_dict=1,
        )	

    def get_rows(self):
        self.data = []		
        self.get_periodic_data()	
        self.get_period_rowweek_ranges()		
        for entity, period_data in iteritems(self.entity_periodic_data):	
            
            row = {
                "entity": entity,
                "entity_name": self.entity_names.get(entity) if hasattr(self, "entity_names") else None,
            }				
            total = 0
            for end_date in self.week_periodic_daterange:				
                period = self.get_weekperiod(end_date)
                amount = flt(period_data.get(period, 0.0))
                row[scrub(period)] = amount
                total += amount

            row["total"] = total	

            self.data.append(row)
    def get_period_rowweek_ranges(self):
        from dateutil.relativedelta import MO, relativedelta

        from_date, to_date = getdate(self.filters.from_date), getdate(self.filters.to_date)

        increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
            self.filters.range, 1
        )

        if self.filters.range in ["Monthly", "Quarterly","Weekly"]:
            from_date = from_date.replace(day=1)
        elif self.filters.range == "Yearly":
            from_date = get_fiscal_year(from_date)[1]
        else:
            from_date = from_date + relativedelta(from_date, weekday=MO(-1))

        self.week_periodic_daterange = []
        for dummy in range(1, 53):
            if self.filters.range == "Weekly":
                period_end_date = add_days(from_date, 6)
            else:
                period_end_date = add_to_date(from_date, months=increment, days=-1)

            if period_end_date > to_date:
                period_end_date = to_date

            self.week_periodic_daterange.append(period_end_date)

            from_date = add_days(period_end_date, 1)
            if period_end_date == to_date:
                break
    
    def get_periodic_data(self):
        self.entity_periodic_data = frappe._dict()		
        if self.filters.range == "Weekly":
            for d in self.entries:				
                period = self.get_weekperiod(d.get(self.date_field))				
                self.entity_periodic_data.setdefault(d.entity, frappe._dict()).setdefault(period.split('@')[0], 0.0)
                self.entity_periodic_data[d.entity][period.split('@')[0]] += flt(d.value_field)

                # if self.filters.tree_type == "Item":
                # 	self.entity_periodic_data[d.entity]["stock_uom"] = d.stock_uom

    def get_period(self, posting_date):			
        calendar.setfirstweekday(5)
        if self.filters.range == "Weekly":
            mnthname= posting_date.strftime('%b')
            x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
            week_of_month = np.where(x == posting_date.day)[0][0] + 1			
            #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)
            period = mnthname +"-"+ str(posting_date.year)[-2:]			
            # elif self.filters.range == "Monthly":
            # 	period = str(self.months[posting_date.month - 1]) + " " + str(posting_date.year)
            # elif self.filters.range == "Quarterly":
            # 	period = "Quarter " + str(((posting_date.month - 1) // 3) + 1) + " " + str(posting_date.year)
            # else:
            # 	year = get_fiscal_year(posting_date, company=self.filters.company)
            # 	period = str(year[0])		
        return period

    def get_weekperiod(self, posting_date):
        calendar.setfirstweekday(5)
        if self.filters.range == "Weekly":
            mnthname= posting_date.strftime('%b')
            x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
            week_of_month = np.where(x == posting_date.day)[0][0] + 1			
            #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)			
            weekperiod= "Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]	
        return weekperiod	

    #for setting column month or week wise
    def get_period_date_ranges(self):
        from dateutil.relativedelta import MO, relativedelta

        from_date, to_date = getdate(self.filters.from_date), getdate(self.filters.to_date)

        increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
            self.filters.range, 1
        )
        #print("c12")

        if self.filters.range in ["Monthly", "Quarterly","Weekly"]:
            from_date = from_date.replace(day=1)
        elif self.filters.range == "Yearly":
            from_date = get_fiscal_year(from_date)[1]
        else:
            from_date = from_date + relativedelta(from_date, weekday=MO(-1))

        self.periodic_daterange = []
        for dummy in range(1, 53):
            if self.filters.range == "Week":
                period_end_date = add_days(from_date, 6)
            else:
                period_end_date = add_to_date(from_date, months=increment, days=-1)

            if period_end_date > to_date:
                period_end_date = to_date

            self.periodic_daterange.append(period_end_date)

            from_date = add_days(period_end_date, 1)
            if period_end_date == to_date:
                break
    
sales_allrecord=[]
@frappe.whitelist()
def get_weekly_report_record(report_name,filters):
    from dateutil.relativedelta import MO, relativedelta
    # Skipping total row for tree-view reports
    skip_total_row = 0
    #return self.columns, self.data, None, None, skip_total_row
    

    filterDt= json.loads(filters)	
    filters = frappe._dict(filterDt or {})	
    
    if filters.to_date:
        end_date= filters.to_date
    else:
        end_date= date.today()
    
    fiscalyeardt= fetchselected_fiscalyear(end_date)
    for fy in fiscalyeardt:
        start_date=fy.get('year_start_date').strftime('%Y-%m-%d')
        fiscal_endDt=fy.get('year_end_date').strftime('%Y-%m-%d')

    filters.update({"fiscal_endDt":fiscal_endDt})
    filters.update({"from_date":start_date})
    
    #######
    coycostcenters = getcostcenters(filters)
    
    fiscalyeardtprev, prevyrsstartdate = fetch5yrsback_fiscalyear(5,filters)
    #for fy2 in fiscalyeardtprev:
    #    print(str(fy2.year) + ' ' + str(fy2.year_start_date) + ' ' + str(fy2.year_end_date) )
    #######

    #print("c13")
    if filters.cost_center:	
        sales_allrecord = frappe.db.sql(
            """
            select X.* from (select 'Consolidated' as entity, i.base_amount as value_field, s.transaction_date, 
            s.company from `tabSales Order` s,
            `tabSales Order Item` i where s.name = i.parent and s.status <> 'Cancelled'and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
            and  s.cost_center = %(cost_center)s
            and s.transaction_date between  %(from_date)s and %(to_date)s 				
            union
            select s.cost_center as entity, i.base_amount as value_field, s.transaction_date,s.company
            from `tabSales Order` s,`tabSales Order Item` i 
            where s.name = i.parent and 
            s.status <> 'Cancelled' 
            and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
            and  s.cost_center = %(cost_center)s
            and s.transaction_date between  %(from_date)s and %(to_date)s 
            ) X
            """, {
                    'cost_center': filters.cost_center,'from_date': filters.from_date,'to_date': end_date				
                },		
                as_dict=1,
            )	
        min_date_backlog = frappe.db.sql(
                """
                select M.*
                    from (select CONCAT(DATE_FORMAT(s.transaction_date, %(b)s),"-", RIGHT(fy.year,2)) as Date,
                    fy.year as year,
                    sum(i.base_amount) AS TotalAmt, 'Consolidated' as cost_center
                    from `tabSales Order` s,`tabSales Order Item` i, `tabFiscal Year` fy 
                    where s.name = i.parent and 
                    s.status <> 'Cancelled'
                    and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
                    and s.transaction_date < %(before_date)s and s.transaction_date >= %(prevstart_date)s
                    and s.transaction_date >= fy.year_start_date and s.transaction_date <= fy.year_end_date 		
                    and  s.cost_center = %(cost_center)s			
                    group by month(s.transaction_date), fy.year					
                    UNION
                    select CONCAT(DATE_FORMAT(s.transaction_date, %(b)s),"-", RIGHT(fy.year,2)) as Date,
                    fy.year as year,
                    sum(i.base_amount) AS TotalAmt, s.cost_center
                    from `tabSales Order` s,`tabSales Order Item` i , `tabFiscal Year` fy
                    where s.name = i.parent and 
                    s.status <> 'Cancelled'
                    and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
                    and s.transaction_date < %(before_date)s and s.transaction_date >= %(prevstart_date)s
                    and s.transaction_date >= fy.year_start_date and s.transaction_date <= fy.year_end_date 
                    and  s.cost_center = %(cost_center)s
                    group by month(s.transaction_date), fy.year,s.cost_center					
                                
                ) M	
            """, {
                    'before_date': start_date	,'b':'%b','cost_center': filters.cost_center,'Y':'%y','prevstart_date' : prevyrsstartdate			
                },		
                as_dict=1,
            )			
    else:
        sales_allrecord = frappe.db.sql(
            """
            select X.*
            from (select 'Consolidated' as entity, i.base_amount as value_field, s.transaction_date, s.company from `tabSales Order` s,
            `tabSales Order Item` i where s.name = i.parent and s.status <> 'Cancelled'and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
            and s.transaction_date between  %(from_date)s and %(to_date)s 			
            UNION
            select s.cost_center as entity, i.base_amount as value_field, s.transaction_date, s.company
            from `tabSales Order` s,`tabSales Order Item` i 
            where s.name = i.parent and 
            s.status <> 'Cancelled'
            and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
            and s.transaction_date between  %(from_date)s and %(to_date)s 
                ) X
            """, {
                    'from_date': start_date,'to_date': filters.to_date				
                },		
                as_dict=1,
            )	
        min_date_backlog = frappe.db.sql(
                    """
                    select M.*
                    from (select CONCAT(DATE_FORMAT(s.transaction_date, %(b)s),"-", RIGHT(fy.year,2)) as Date,
                    fy.year as year,
                    sum(i.base_amount) AS TotalAmt, 'Consolidated' as cost_center
                    from `tabSales Order` s,`tabSales Order Item` i, `tabFiscal Year` fy 
                    where s.name = i.parent and 
                    s.status <> 'Cancelled'
                    and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
                    and s.transaction_date < %(before_date)s and s.transaction_date >= %(prevstart_date)s
                    and s.transaction_date >= fy.year_start_date and s.transaction_date <= fy.year_end_date
                    group by month(s.transaction_date), fy.year					
                    UNION
                    select CONCAT(DATE_FORMAT(s.transaction_date, %(b)s),"-", RIGHT(fy.year,2)) as Date,
                    fy.year as year,
                    sum(i.base_amount) AS TotalAmt, s.cost_center
                    from `tabSales Order` s,`tabSales Order Item` i, `tabFiscal Year` fy 
                    where s.name = i.parent and 
                    s.status <> 'Cancelled'
                    and s.delivery_status='Not Delivered' and s.billing_status='Not Billed'
                    and s.transaction_date < %(before_date)s and s.transaction_date >= %(prevstart_date)s
                    and s.transaction_date >= fy.year_start_date and s.transaction_date <= fy.year_end_date
                    group by month(s.transaction_date), fy.year,s.cost_center					
                    ) M 						
                """, {
                        'before_date': start_date	,'b':'%b','Y':'%y','prevstart_date' : prevyrsstartdate				
                    },		
                    as_dict=1,
                )	
    year_total_list = frappe._dict()	
    for dd in min_date_backlog:		
        #print(dd)				
        year_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(dd.year,frappe._dict()).setdefault(dd.Date,dd.TotalAmt)						
        year_total_list[dd.cost_center][dd.year][dd.Date] += flt(dd.TotalAmt)	
    #print(year_total_list)    

    
    # check through all cost centers and prev yrs and see missing months and year and initialize to zero
    year_total_list2 = frappe._dict()
    for fy3 in fiscalyeardtprev: 
        fyr = fy3.year
        fsd = fy3.year_start_date
        fed = fy3.year_end_date
        currdt = fy3.year_start_date
        bkltotamt = 0.0
        #for x in range(1, 12):
        i = 1
        while ((i < 13) and (currdt < fed)):
            i += 1
            #print(currdt)
            mthyrstr = currdt.strftime("%b") + "-" + fyr[-2:]
            #print(mthyrstr)
            #take care of consolidated cost center
            #loop through all cost centers
            consolidatedcc = 'Consolidated'
            ccTotalAmt0 = 0
            for dd in min_date_backlog:
                if ((dd.cost_center==consolidatedcc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                    ccTotalAmt0 = dd.TotalAmt
            year_total_list2.setdefault(consolidatedcc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt0)
            year_total_list2[consolidatedcc][fyr][mthyrstr] += flt(ccTotalAmt0)
            #
            for cc in coycostcenters:
                ccTotalAmt = 0
                for dd in min_date_backlog:
                    if ((dd.cost_center==cc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                        ccTotalAmt = dd.TotalAmt
                year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)            


            currdt2 = currdt + relativedelta(months=+1)
            currdt = currdt2


    #print(year_total_list2)        
    
    #year_lis = list(year_total_list.items())  #convert dict to list
    year_lis = list(year_total_list2.items())
    
    WSobj = WeeklySales()
    WSobj.__init__()	
    compnyName=""	
    if sales_allrecord:
        ftch_cmpny = {entry.get('company') for entry in sales_allrecord}		
        compnyName=ftch_cmpny
        
    Cust_periodic_daterange=cust_get_period_date_ranges(filters)
    Cust_colum_name=cust_get_columns(filters,Cust_periodic_daterange)
    #print(Cust_colum_name)	
    #Cust_rows_values=cust_get_rows(filters,sales_allrecord,Cust_periodic_daterange)
    Cust_rows_values=cust_get_rows_forallweeks(filters,sales_allrecord,Cust_periodic_daterange,coycostcenters,start_date,fiscal_endDt)
    #print(Cust_rows_values)
    combined_list=[]
    combined_list.append((list(Cust_rows_values), year_lis))
    #print(combined_list)
    #print(year_lis)	
    #print(Cust_rows_values)

    return Cust_colum_name,combined_list,compnyName	

#
def cust_get_columns(filters,Cust_periodic_daterange):
    cust_columns=[
            {
                "label": "Backlog", 
                "fieldname": "Backlog", 
                "fieldtype": "data",
                    "width": 120
            }
        ]
    for end_date in Cust_periodic_daterange:
        period = cust_get_period(end_date,filters)							
        cust_columns.append(
            {
                "label": _(period), 
                "fieldname": scrub(period), 
                "fieldtype": "Float",
                    "width": 120
            }
        )
    # cust_columns.append(
    # 	{"label": _("Total"), "fieldname": "total", "fieldtype": "Float", "width": 120}
    # )
    #print("c14")
    return cust_columns

#
def cust_get_period(posting_date,filters):
    period = ""
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        mnthname= posting_date.strftime('%b')
        x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
        week_of_month = np.where(x == posting_date.day)[0][0] + 1			
        #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)
        period = mnthname +"-"+ str(posting_date.year)[-2:]			
        # elif self.filters.range == "Monthly":
        # 	period = str(self.months[posting_date.month - 1]) + " " + str(posting_date.year)
        # elif self.filters.range == "Quarterly":
        # 	period = "Quarter " + str(((posting_date.month - 1) // 3) + 1) + " " + str(posting_date.year)
        # else:
        # 	year = get_fiscal_year(posting_date, company=self.filters.company)
        # 	period = str(year[0])
    #print("c15")		
    return period

#for setting column month from week wise
def cust_get_period_date_ranges(filters):
    from dateutil.relativedelta import MO, relativedelta	
    from_date, to_date = getdate(filters.from_date), getdate(filters.fiscal_endDt)

    increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
        filters.range, 1
    )
    
    
    #print("c16")
    if filters.range in ["Monthly", "Quarterly","Weekly"]:
        from_date = get_fiscal_year(from_date)[1]
    elif filters.range == "Yearly":
        from_date = get_fiscal_year(from_date)[1]
    else:
        from_date = from_date + relativedelta(from_date, weekday=MO(-1))

    periodic_daterange = []
    for dummy in range(1, 53):
        if filters.range == "Week":
            period_end_date = add_days(from_date, 6)
        else:
            period_end_date = add_to_date(from_date, months=increment, days=-1)

        if period_end_date > to_date:
            period_end_date = to_date

        periodic_daterange.append(period_end_date)

        from_date = add_days(period_end_date, 1)
        if period_end_date == to_date:
            break
    return periodic_daterange

#
def cust_get_weekperiod(filters, posting_date):
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        mnthname= posting_date.strftime('%b')
        x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 		
        week_of_month = np.where(x == posting_date.day)[0][0] + 1			
        #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)			
        weekperiod= "Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]
    #print("c17")	
    return weekperiod

def cust_get_allweekperiods(filters, start_date, end_date):
    from dateutil.relativedelta import relativedelta
    data = []
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        currdt = getdate(start_date)
        while (currdt <= getdate(end_date)):
            for x in range(1, 6):
                mnthname= currdt.strftime('%b')
                weekperiod= "Week " + str(x) +"@"+mnthname +"-"+ str(currdt.year)[-2:]
                data.append(weekperiod)
            currdt2 = currdt + relativedelta(months=+1)
            currdt = currdt2
    #print(data)    	
    return data


#bind rows according to the record
def cust_get_rows(filters,records,Cust_periodic_daterange):
    data = []	
    ## start get week from month
    entity_periodic_data = frappe._dict()	
    if filters.range == "Weekly":			
        for d in records:								
            cust_period = cust_get_weekperiod(filters,d.transaction_date)				
            entity_periodic_data.setdefault(d.entity, frappe._dict()).setdefault(cust_period,0.0)						
            entity_periodic_data[d.entity][cust_period] += flt(d.value_field)			
        
    con_lis = list(entity_periodic_data.items())  #convert dict to list
    
    #print("c18")
    return con_lis

def cust_get_rows_forallweeks(filters,records,Cust_periodic_daterange,coycostcenters,from_date, to_date):
    data = []	
    ## start get week from month
    entity_periodic_data = frappe._dict()	
    if filters.range == "Weekly":
        # set all week periods
        cust_periods_list = cust_get_allweekperiods(filters, from_date, to_date)
        consolidcc = "Consolidated"
        for cp in cust_periods_list:
            ccTotalAmt0 = 0.0
            for d in records:
                cust_period = cust_get_weekperiod(filters,d.transaction_date)
                if ((consolidcc==d.entity) and (cp==cust_period)):
                    ccTotalAmt0 += flt(d.value_field)
                        
            entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,ccTotalAmt0)
        
        for cc in coycostcenters:
            for cp in cust_periods_list:
                ccTotalAmt = 0.0
                for d in records:
                    cust_period = cust_get_weekperiod(filters,d.transaction_date)
                    if ((cc==d.entity) and (cp==cust_period)):
                        ccTotalAmt += flt(d.value_field)
                        
                entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,ccTotalAmt)						
            
    con_lis = list(entity_periodic_data.items())  #convert dict to list
    
    #print(con_lis)
    return con_lis    

#
def fetchselected_fiscalyear(end_date):
    fetch_fiscalyearslctn = frappe.db.sql(
        """
        (select year_start_date , year_end_date from `tabFiscal Year` 
        where  %(Slct_date)s between year_start_date and year_end_date
        )	
    """,{
            'Slct_date': end_date
        },		
        as_dict=1,
    )
    #print("c19")			
    return fetch_fiscalyearslctn

def fetch5yrsback_fiscalyear(noofyrsback,filters):
    if filters.to_date:
        end_date= filters.to_date
    else:
        end_date= date.today()
    fetch_fiscalyearslctn_1 = frappe.db.sql(
        """
        (select year, year_start_date , year_end_date from `tabFiscal Year` 
        where  %(Slct_date)s between year_start_date and year_end_date
        )	
    """,{
            'Slct_date': end_date
        },		
        as_dict=1,
    )
    curryr = 0
    prevyrsback = 0
    for ff in fetch_fiscalyearslctn_1:		
        #print(dd)				
        curryr = ff.year						
    prevyrsback = int(curryr) - noofyrsback

    fetch_fiscalyearslctn = frappe.db.sql(
        """
        (select year, year_start_date , year_end_date from `tabFiscal Year` 
        where year >= %(startyr)s and year < %(endyr)s order by year asc
        )	
    """,{
            'startyr': prevyrsback, 'endyr': curryr
        },		
        as_dict=1,
    )  

    fetch_fiscalyearslctn_3 = frappe.db.sql(
        """
        (select min(year_start_date) as begindate from `tabFiscal Year` 
        where year >= %(startyr)s and year < %(endyr)s
        )	
    """,{
            'startyr': prevyrsback, 'endyr': curryr
        },		
        as_dict=1,
    )      
    for ff2 in fetch_fiscalyearslctn_3:		
        prevyrsstartdate = ff2.begindate				
    #print(prevyrsstartdate)			
    return fetch_fiscalyearslctn,prevyrsstartdate

def getcostcenters(filters):
    cstcnt = [] # get function to fetch cost centers
    cstcnt0 = frappe.db.get_list("Cost Center",pluck='name',filters={'company': filters.company,'is_group':0})
    # change the order of cost center this is customized for this client
    #specify order here 02, 03, 01, 06
    #cstorder = []
    cstorder = ['02', '03', '06', '01']
    i = 0
    while(i<len(cstorder)):
        for cstr in cstcnt0:
            if (cstr.startswith(cstorder[i])):
                cstcnt.append(cstr)
        i+=1
        
    # if created cost centers increase
    if ((len(cstorder)<len(cstcnt0)) and (len(cstcnt)>0) ):
        for cstr2 in cstcnt0:
            cstfound = False
            for m in cstcnt:
                if (m==cstr2):
                    cstfound = True
            if (cstfound == False):
                 cstcnt.append(cstr2)         

    if (len(cstcnt)==0):
       cstcnt = cstcnt0 
    return cstcnt			