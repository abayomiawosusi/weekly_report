// Copyright (c) 2022, abayomi.awosusi@sgatechsolutions.com and contributors
// For license information, please see license.txt
/* eslint-disable */

frappe.query_reports["Weekly Sales Report"] = {
	"filters": [
		{
            fieldname: 'company',
            label: __('Company'),
            fieldtype: 'Link',
            options: 'Company',
            default: frappe.defaults.get_user_default('company'),
			hidden: 1
        },
		{
			fieldname: "from_date",
			label: __("From Date"),
			fieldtype: "Date",
			hidden: 1
		},
		{
			fieldname:"to_date",
			label: __("To Date"),
			fieldtype: "Date",
			default: frappe.defaults.get_user_default("year_end_date"),
			reqd: 1,				
		},
		{
			fieldname: "range",
			label: __("Range"),
			fieldtype: "Select",
			options: [
				{ "value": "Weekly", "label": __("Weekly") },
				{ "value": "Monthly", "label": __("Monthly") }
			],
			default: "Weekly",
			reqd: 1,
			hidden: 1
		},
		{
			fieldname: "cost_center",
			label: __("Cost Center"),
			fieldtype: "MultiSelectList",
			options: "Cost Center",
			reqd:0,				
			get_data: function(txt) {				
				return frappe.db.get_link_options("Cost Center", txt);
			}
		},
		{
			fieldname:"fiscal_endDt",
			label: __("Fiscal EndDate"),
			fieldtype: "Date",
			default: frappe.defaults.get_user_default("year_end_date"),
			hidden: 1,				
		},
		{
			fieldname: "fiscalyr",
			label: __("Year"),
			fieldtype: "Int",
			hidden: 1
		},

	],
	onload: function (report) {
		report.page.add_inner_button(__("Export Report"), function () {
			debugger
			let filters = report.get_values();

			frappe.call({
				method: 'weekly_report.weekly_report.report.weekly_sales_report.weekly_sales_report.get_weekly_report_record',

				args: {
					report_name: report.report_name,
					filters: filters
				},

				callback: function (r) {
					$(".report-wrapper").html("");
					$(".justify-center").remove()
                    //console.log(r.message[3]) #selected date
					//console.log(r)
					if (r.message[2] != "") {
						//var fiscal_endDt = new Date(frappe.defaults.get_user_default("year_end_date"))
						var fiscal_endDt = new Date(r.message[3])
						var months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
						var FDay = fiscal_endDt.getDate() + 1;
						var FYear = fiscal_endDt.getFullYear();
						var monthName = months[fiscal_endDt.getMonth()];
						var EndYearDate = monthName + " " + FDay + ", " + FYear;
						dynamic_exportcontent(r.message, EndYearDate, filters.cost_center ? filters.cost_center : "")
					} else {
						alert("No record found.")
					}
				}
			});
		});
	},
}

// populates the spreadsheet and exports as .xls
function dynamic_exportcontent(cnt_list, EDate, cost_center) {
	var dynhtml = '<div id="dvData">';
	var totlcnt = [];
	var lstCstCntr = [];

	$.each(cnt_list[1][0][0], function (gbl_ind, gbl_val) {
		var $crntid = "exprtid_" + gbl_ind;
		totlcnt[gbl_ind] = "#" + $crntid;
		lstCstCntr[gbl_ind] = gbl_val[0];
		const Col_list = cnt_list[0];
		const divcol = Math.ceil(cnt_list[0].length / 2);

		dynhtml += '<table id=' + $crntid + '>';

		dynhtml += '<tr><td>';

		const collist = [];
		collist[0] = Col_list.slice(1, divcol);
		collist[1] = Col_list.slice(divcol);

		//dynhtml += '<table id=' + $crntid + '_0' + '>';

		//dynhtml += '<table id=' + $crntid + '>';

        dynhtml += '<table id=' + $crntid + '_0' + '>';

		var compnyName = "";
		var trmcc = lstCstCntr[gbl_ind];
		/*var spltcostcntr = (trmcc).split('-');
        
			if (spltcostcntr.length > 1) {
				spltcostcntr.shift();
				spltcostcntr.pop()

			}
        */
		var spltcostcntr = (trmcc).slice(5);
		var spltcostcntr1 = spltcostcntr.substr(0, (spltcostcntr.length)-5)
        	

		compnyName = gbl_ind == 0 ? "Consolidated" : spltcostcntr1; //cnt_list[2];

		dynhtml += '<caption><span style="font-weight: bold;">Company Name- ' + compnyName + '</br>Weekly Backlog Report</br>For the Year ending ' + EDate + '</span><caption>';
		for (var cnt = 0; cnt < collist.length; cnt++) {
			//first half columns
			dynhtml += '<tr></tr>';
			dynhtml += '<tr>';
			dynhtml += '<td width="100" style="font-weight: bold;text-decoration: underline;">Backlog</td>';
			for (var cl = 0; cl < collist[cnt].length; cl++) {
				var colmnth = collist[cnt][cl].label.toString();
				dynhtml += '<td width="100" style="text-align: center;border: 1px solid #89898d;font-weight: bold;"><span style="color:white;">"</span>' + (colmnth).toString() + '<span style="color:white;">"</span></td>';
			}
			dynhtml += '</tr>';
			dynhtml += '<tr>' + (row_celldynFunc(gbl_val, "Week 1", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFunc(gbl_val, "Week 2", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFunc(gbl_val, "Week 3", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFunc(gbl_val, "Week 4", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFunc(gbl_val, "Week 5", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (totalAmt_permthweeks(gbl_val, collist[cnt])) + '</tr>';
			dynhtml += (year_totalamt(cnt_list[1][0][1], gbl_val[0], collist[cnt]))
			dynhtml += '<td colspan="7"></td>';
			dynhtml += '</tr>';
		}
		dynhtml += '</table>'

		dynhtml += '</td></tr>';

		dynhtml += '<tr><td>';
		dynhtml += generatesalestable(cnt_list,$crntid,compnyName,EDate,gbl_ind);
		
		dynhtml += '</td></tr>';

		dynhtml += '</table>';
	})

	dynhtml += '</div>';
	console.log(dynhtml)
	$(".report-wrapper").hide();
	$(".report-wrapper").append(dynhtml);
	tablesToExcel(totlcnt, lstCstCntr, 'SalesWeeklyReport.xls')
}

//cum for year is = sum(monthsales * monthgrossmargin)/sum(monthsales)
function generatesalestable(cnt_list,$crntid,compnyName,EDate,gbl_ind) {
	var dynhtml = '';
	gbl_val = cnt_list[5][0][0][gbl_ind]
	//$.each(cnt_list[5][0][0], function (gbl_ind, gbl_val) {
		//var $crntid = "exprtid_" + gbl_ind;
		//totlcnt[gbl_ind] = "#" + $crntid;
		//lstCstCntr[gbl_ind] = gbl_val[0];
		const Col_list = cnt_list[4];
		//const divcol = Math.ceil(cnt_list[4].length / 4);
		divcol = 8;

		const collist = [];
		collist[0] = Col_list.slice(1, 9);
		collist[1] = Col_list.slice(9,17);
		collist[2] = Col_list.slice(17,25);
		collist[3] = Col_list.slice(-2);

		dynhtml += '<table id=' + $crntid + '_2' + '>';
		//var compnyName = "";
		//var trmcc = lstCstCntr[gbl_ind];
		/*var spltcostcntr = (trmcc).split('-');
        
			if (spltcostcntr.length > 1) {
				spltcostcntr.shift();
				spltcostcntr.pop()

			}
        */
		//var spltcostcntr = (trmcc).slice(5);
		//var spltcostcntr1 = spltcostcntr.substr(0, (spltcostcntr.length)-5)
        	

		//compnyName = gbl_ind == 0 ? "Consolidated" : spltcostcntr1; //cnt_list[2];

		dynhtml += '<caption><span style="font-weight: bold;">Company Name- ' + compnyName + '</br>Weekly Sales Report</br>For the Year ending ' + EDate + '</span><caption>';
		for (var cnt = 0; cnt < collist.length; cnt++) {
			//first half columns
			dynhtml += '<tr></tr>';
			dynhtml += '<tr>';
			dynhtml += '<td width="100" style="font-weight: bold;text-decoration: underline;">Sales</td>';
			for (var cl = 0; cl < collist[cnt].length; cl++) {
				var colmnth = collist[cnt][cl].label.toString();
				dynhtml += '<td width="100" style="text-align: center;border: 1px solid #89898d;font-weight: bold;"><span style="color:white;">"</span>' + (colmnth).toString() + '<span style="color:white;">"</span></td>';
			}
			dynhtml += '</tr>';
			dynhtml += '<tr>' + (row_celldynFuncsales(gbl_val, "Week 1", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFuncsales(gbl_val, "Week 2", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFuncsales(gbl_val, "Week 3", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFuncsales(gbl_val, "Week 4", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (row_celldynFuncsales(gbl_val, "Week 5", collist[cnt])) + '</tr>';
			dynhtml += '<tr>' + (totalAmt_permthweekssales(gbl_val, collist[cnt])) + '</tr>';
			dynhtml += (year_totalamtsales(cnt_list[5][0][1], gbl_val[0], collist[cnt]))
			//dynhtml += '<td colspan="7"></td>';
			dynhtml += '</tr>';
		}
		dynhtml += '</table>'
	///})
    //console.log(dynhtml)
	return dynhtml;
}

function formatAsPercent(num) {
	return new Intl.NumberFormat('default', {
	  style: 'percent',
	  minimumFractionDigits: 2,
	  maximumFractionDigits: 2,
	}).format(num / 100);
}

// filling cell values dynamically according to the selection
function row_celldynFunc(cnt_list, weekNo, colDt) {
	celldynhtml = '<td style="">' + weekNo + '</td>';
	for (var col = 0; col < colDt.length; col++) {
		var iscol_exist = false
		var gblweekprice = 0
		$.each(cnt_list[1], function (ind, val) {
			var fetchweek = ind.split('@')[0];
			var fetchmnth = ind.split('@')[1];

			if (fetchmnth == colDt[col].label) {
				if (fetchweek.toLowerCase() != "") {
					if (fetchweek.toLowerCase() == weekNo.toLowerCase()) {
						iscol_exist = true
						gblweekprice = val
					}
				}
			}
		})

		if (iscol_exist == true) {
			celldynhtml += '<td style="border: 1px solid #89898d;">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
		} else {
			celldynhtml += '<td style="border: 1px solid #89898d;">$0</td>';
		}
	}
	return celldynhtml;
}

function row_celldynFuncsales(cnt_list, weekNo, colDt) {
	celldynhtml = '<td style="">' + weekNo + '</td>';
	for (var col = 0; col < colDt.length; col++) {
		var iscol_exist = false
		var gblweekprice = 0
		var gblweekpricemargin = 0.0
		$.each(cnt_list[1], function (ind, val) {
			var fetchweek = ind.split('@')[0];
			var fetchmnth = ind.split('@')[1];

			if (fetchmnth == colDt[col].label) {
				if (fetchweek.toLowerCase() != "") {
					if (fetchweek.toLowerCase() == weekNo.toLowerCase()) {
						iscol_exist = true
						gblweekprice = val.sales
						gblweekpricemargin = val.grossprofitmargin
					}
				}
			}
		})

		if (iscol_exist == true) {
			celldynhtml += '<td style="border: 1px solid #89898d;">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
			celldynhtml += '<td style="border: 1px solid #89898d;">' + (gblweekpricemargin).toLocaleString() + '%</td>';
		} else {
			celldynhtml += '<td style="border: 1px solid #89898d;">$0</td>';
			celldynhtml += '<td style="border: 1px solid #89898d;">0.0%</td>';
		}
	}
	return celldynhtml;
}

// total amount of week according to month
function totalAmt_permthweeks(cnt_list, colDt) {
	celldynhtml =  '<td style="font-weight: bold;">Mth End</td>';

	for (var col = 0; col < colDt.length; col++) {
		var iscol_exist = false
		var gblweekprice = 0
		$.each(cnt_list[1], function (ind, val) {
			var fetchweek = ind.split('@')[0];
			var fetchmnth = ind.split('@')[1];

			if (fetchmnth == colDt[col].label) {
				if (fetchweek.toLowerCase() != "") {
					gblweekprice = val;
					iscol_exist = true;
				}
			}
		})

		if (iscol_exist == true) {
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
		} else {
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">$0</td>';
		}
	}
	return celldynhtml;
}

function totalAmt_permthweekssales(cnt_list, colDt) {
	celldynhtml =  '<td style="font-weight: bold;">Mth End</td>';

	for (var col = 0; col < colDt.length; col++) {
		var iscol_exist = false
		var gblweekprice = 0
		var gblweekprofit = 0
		var gblweekpricemargin = 0.0
		$.each(cnt_list[1], function (ind, val) {
			var fetchweek = ind.split('@')[0];
			var fetchmnth = ind.split('@')[1];

			if (fetchmnth == colDt[col].label) {
				if (fetchweek.toLowerCase() != "") {
					gblweekprice += val.sales
				    gblweekprofit = val.grossprofit
					iscol_exist = true;
				}
			}
		})
		if (gblweekprice != 0) 
		{
         gblweekpricemargin = formatAsPercent(gblweekprofit/gblweekprice);
        } 		 
		if (iscol_exist == true) {
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">' + (gblweekpricemargin).toLocaleString() + '%</td>';
		} else {
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">$0</td>';
			celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;">0.0%</td>';
		}
	}
	return celldynhtml;
}

// total of year amount
function year_totalamt(cnt_list, cstcntrNm, colDt) {
	celldynhtml = "";

	$.each(cnt_list, function (rind, rval) {
		if (rval[0].toLowerCase() == cstcntrNm.toLowerCase()) {
			var revrselist = [];
			var mmm = Object.keys(rval[1])
				.reverse()
				.forEach(function (v, i) {
					revrselist.push({
						key: v,
						value: rval[1][v]
					});
				});
			var colors = ["red", "blue", "green", "#E75480", "#c21010", "#5480E8"]
			$.each(revrselist, function (rm_indx, rm_val) {
				celldynhtml += '<tr>';

				celldynhtml += '<td style="font-weight: bold;text-align: left;border-color:#89898d;color:' + colors[rm_indx] + '">' + rm_val.key + '</td>';
				for (var col = 0; col < colDt.length; col++) {
					var iscol_exist = false
					var gblweekprice = 0
					$.each(revrselist, function (m_indx, m_val) {
						$.each(m_val.value, function (pm_indx, pm_val) {
							var fetchmnth = pm_indx.split('-');
							var crntyr = (rm_val.key).slice(-2);
							if (crntyr == fetchmnth[1]) {
								if (fetchmnth[0].toLowerCase() == colDt[col].label.split('-')[0].toLowerCase()) {
									gblweekprice += pm_val;
									iscol_exist = true;
								}
							}
						})
					})
					if (iscol_exist == true) {
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
					}
					else {
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">$0</td>';
					}
				}
				celldynhtml += '</tr>';
			})
		}
	})
	return celldynhtml;
}

function year_totalamtsales(cnt_list, cstcntrNm, colDt) {
	var celldynhtml = "";

	$.each(cnt_list, function (rind, rval) {
		if (rval[0].toLowerCase() == cstcntrNm.toLowerCase()) {
			var revrselist = [];
			var mmm = Object.keys(rval[1])
				.reverse()
				.forEach(function (v, i) {
					revrselist.push({
						key: v,
						value: rval[1][v]
					});
				});
			var colors = ["red", "blue", "green", "#E75480", "#c21010", "#5480E8"]
			$.each(revrselist, function (rm_indx, rm_val) {
				celldynhtml += '<tr>';

				celldynhtml += '<td style="font-weight: bold;text-align: left;border-color:#89898d;color:' + colors[rm_indx] + '">' + rm_val.key + '</td>';
				for (var col = 0; col < colDt.length; col++) {
					var iscol_exist = false
					var gblweekprice = 0
					var gblweekpricemargin = 0
					$.each(revrselist, function (m_indx, m_val) {
						$.each(m_val.value, function (pm_indx, pm_val) {
							var fetchmnth = pm_indx.split('-');
							var crntyr = (rm_val.key).slice(-2);
							if (crntyr == fetchmnth[1]) {
								if (fetchmnth[0].toLowerCase() == colDt[col].label.split('-')[0].toLowerCase()) {
									gblweekprice += pm_val.sales;
									gblweekpricemargin += pm_val.grossprofitmargin;
									iscol_exist = true;
								}
							}
						})
					})
					if (iscol_exist == true) {
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">$' + (Math.round(gblweekprice)).toLocaleString() + '</td>';
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">' + (gblweekpricemargin).toLocaleString("en-CA", { maximumFractionDigits: 2, minimumFractionDigits: 2 }) + '</td>';
					}
					else {
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">$0</td>';
						celldynhtml += '<td style="border: 1px solid #89898d;font-weight: bold;border-color: #89898d;color:' + colors[rm_indx] + '">0.0%</td>';
					}
				}
				celldynhtml += '</tr>';
			})
		}
	})
	return celldynhtml;
}

// outlines the formatting for the excel file
var tablesToExcel = (function () {
	var uri = 'data:application/vnd.ms-excel;base64,',
		html_start = `<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">`,
		template_ExcelWorksheet = `<x:ExcelWorksheet><x:Name>{SheetName}</x:Name><x:WorksheetSource HRef="sheet{SheetIndex}.htm"/></x:ExcelWorksheet>`,
		template_ListWorksheet = `<o:File HRef="sheet{SheetIndex}.htm"/>`,
		template_HTMLWorksheet = `
------=_NextPart_dummy
Content-Location: sheet{SheetIndex}.htm
Content-Type: text/html; charset=windows-1252

` + html_start + `
<head>
<meta http-equiv="Content-Type" content="text/html; charset=windows-1252">
<link id="Main-File" rel="Main-File" href="../WorkBook.htm">
<link rel="File-List" href="filelist.xml">
</head>
<body><table>{SheetContent}</table></body>
</html>`,

		template_WorkBook = `MIME-Version: 1.0
X-Document-Type: Workbook
Content-Type: multipart/related; boundary="----=_NextPart_dummy"

------=_NextPart_dummy
Content-Location: WorkBook.htm
Content-Type: text/html; charset=windows-1252

` + html_start + `
<head>
<meta name="Excel Workbook Frameset">
<meta http-equiv="Content-Type" content="text/html; charset=windows-1252">
<link rel="File-List" href="filelist.xml">
<!--[if gte mso 9]><xml>
<x:ExcelWorkbook>
<x:ExcelWorksheets>{ExcelWorksheets}</x:ExcelWorksheets>
<x:ActiveSheet>0</x:ActiveSheet>
</x:ExcelWorkbook>
</xml><![endif]-->
</head>
<frameset>
<frame src="sheet0.htm" name="frSheet">
<noframes><body><p>This page uses frames, but your browser does not support them.</p></body></noframes>
</frameset>
</html>
{HTMLWorksheets}
Content-Location: filelist.xml
Content-Type: text/xml; charset="utf-8"

<xml xmlns:o="urn:schemas-microsoft-com:office:office">
<o:MainFile HRef="../WorkBook.htm"/>
{ListWorksheets}
<o:File HRef="filelist.xml"/>
</xml>
------=_NextPart_dummy--
`,
		base64 = function (s) { 
			return window.btoa(unescape(encodeURIComponent(s))) 
		},
		format = function (s, c) { 
			return s.replace(/{(\w+)}/g, function (m, p) { return c[p]; }) 
		}

	return function (tables, costcntr, filename) {
		var context_WorkBook = {
			ExcelWorksheets: '',
			HTMLWorksheets: '',
			ListWorksheets: ''
		};
		var tables = jQuery(tables);

		$.each(tables, function (SheetIndex, val) {
			var $table = $(val);
			var SheetName = "";
			var spltcostcntr = (costcntr[SheetIndex]).split('-');

			if (spltcostcntr.length > 1) {
				spltcostcntr.shift();
				spltcostcntr.pop()
				$.each(spltcostcntr, function (ind, val) {
					SheetName += val.trim();
				})
			} else { 
				SheetName = spltcostcntr[0]; 
			}

			if ($.trim(SheetName) === '') {
				SheetName = 'Sheet' + SheetIndex;
			}

			context_WorkBook.ExcelWorksheets += format(template_ExcelWorksheet, {
				SheetIndex: SheetIndex,
				SheetName: SheetName
			});

			context_WorkBook.HTMLWorksheets += format(template_HTMLWorksheet, {
				SheetIndex: SheetIndex,
				SheetContent: $table.html()
			});

			context_WorkBook.ListWorksheets += format(template_ListWorksheet, {
				SheetIndex: SheetIndex
			});
		});

		var link = document.createElement("A");
		link.href = uri + base64(format(template_WorkBook, context_WorkBook));
		link.download = filename || 'Workbook.xls';
		link.target = '_blank';
		document.body.appendChild(link);
		link.click();
		document.body.removeChild(link);
	}
})();


