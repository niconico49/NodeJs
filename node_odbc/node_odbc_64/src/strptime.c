/*-
 * Copyright (c) 1997, 1998, 2005, 2008 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code was contributed to The NetBSD Foundation by Klaus Klein.
 * Heavily optimised by David Laight
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */ 

/*
 * Inclusion in node-odbc note:
 * 
 * This code was found here: http://social.msdn.microsoft.com/forums/en-US/vcgeneral/thread/25a654f9-b6b6-490a-8f36-c87483bb36b7
 * One user posted what looks to be a scaled down version of the NetBSD code
 * but did not include any header with their work. Since it seems pretty obvious
 * that the user took much of the code from NetBSD, that is why the NetBSD header 
 * is displayed above. 
 */

#include "strptime.h"
 
static int conv_num(const char **, int *, int, int);
static int strncasecmp(char *s1, char *s2, size_t n);
 
char * strptime(const char *buf, const char *fmt, struct tm *tm) {
	char c;
	const char *bp;
	size_t len = 0;
	int alt_format, i, split_year = 0;
 
	bp = buf;

	while ((c = *fmt) != '\0') {
		// Clear `alternate' modifier prior to new conversion.
		alt_format = 0;

		// Eat up white-space.
		if (isspace(c)) {
			while (isspace(*bp)) {
				bp++;
			}

			fmt++;
			continue;
		}

		if ((c = *fmt++) != '%') {
			goto literal;
		}
 
again:	switch (c = *fmt++) {
			// "%%" is converted to "%".
			case '%':
				literal :
				if (c != *bp++) {
					return (0);
				}
				break;
 
			//"Alternative" modifiers. Just set the appropriate flag and start over again.
			// "%E?" alternative conversion modifier.
			case 'E':
				LEGAL_ALT(0);
				alt_format |= ALT_E;
				goto again;
 
			// "%O?" alternative conversion modifier.
			case 'O': 
				LEGAL_ALT(0);
				alt_format |= ALT_O;
				goto again;
       
			//"Complex" conversion rules, implemented through recursion.
			// Date and time, using the locale's format.
			case 'c':
				LEGAL_ALT(ALT_E);
				if (!(bp = strptime(bp, "%x %X", tm))) {
					return (0);
				}
				break;
			// The date as "%m/%d/%y".
			case 'D':
				LEGAL_ALT(0);
				if (!(bp = strptime(bp, "%m/%d/%y", tm))) {
					return (0);
				}
				break;
			// The time as "%H:%M".
			case 'R': 
				LEGAL_ALT(0);
				if (!(bp = strptime(bp, "%H:%M", tm))) {
					return (0);
				}
				break;
			// The time in 12-hour clock representation.     
			case 'r': 
				LEGAL_ALT(0);
				if (!(bp = strptime(bp, "%I:%M:%S %p", tm))) {
					return (0);
				}
				break;
			// The time as "%H:%M:%S". 
			case 'T': 
				LEGAL_ALT(0);
				if (!(bp = strptime(bp, "%H:%M:%S", tm))) {
					return (0);
				}
				break;
			// The time, using the locale's format.
			case 'X':
				LEGAL_ALT(ALT_E);
				if (!(bp = strptime(bp, "%H:%M:%S", tm))) {
					return (0);
				}
				break;
			// The date, using the locale's format.
			case 'x': 
				LEGAL_ALT(ALT_E);
				if (!(bp = strptime(bp, "%m/%d/%y", tm))) {
					return (0);
				}
				break;
 
			//"Elementary" conversion rules.
			// The day of week, using the locale's form.
			case 'A':
			case 'a':
				LEGAL_ALT(0);
				for (i = 0; i < 7; i++) {
					// Full name.
					len = strlen(day[i]);
					if (strncasecmp((char *)(day[i]), (char *)bp, len) == 0) {
						break;
					}

					// Abbreviated name.
					len = strlen(abday[i]);
					if (strncasecmp((char *)(abday[i]), (char *)bp, len) == 0) {
						break;
					}
				}
 
				// Nothing matched.
				if (i == 7) {
					return (0);
				}
 
				tm->tm_wday = i;
				bp += len;
				break;
			// The month, using the locale's form.
			case 'B':
			case 'b':
			case 'h':
				LEGAL_ALT(0);
				for (i = 0; i < 12; i++) {
					// Full name.
					len = strlen(mon[i]);
					if (strncasecmp((char *)(mon[i]), (char *)bp, len) == 0) {
						break;
					}

					// Abbreviated name.
					len = strlen(abmon[i]);
					if (strncasecmp((char *)(abmon[i]), (char *)bp, len) == 0) {
						break;
					}
				}
 
				// Nothing matched.
				if (i == 12) {
					return (0);
				}
 
				tm->tm_mon = i;
				bp += len;
				break;
			// The century number. 
			case 'C':
				LEGAL_ALT(ALT_E);
				if (!(conv_num(&bp, &i, 0, 99))) {
					return (0);
				}
 
				if (split_year)	{
					tm->tm_year = (tm->tm_year % 100) + (i * 100);
				}
				else {
					tm->tm_year = i * 100;
					split_year = 1;
				}
				break;
			// The day of month. 
			case 'd':
			case 'e':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_mday, 1, 31))) {
					return (0);
				}
				break;
			// The hour (24-hour clock representation). 
			case 'k':
				LEGAL_ALT(0);
				// FALLTHROUGH
			case 'H':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_hour, 0, 23))) {
					return (0);
				}
				break;
			// The hour (12-hour clock representation).
			case 'l':
				LEGAL_ALT(0);
				// FALLTHROUGH
			case 'I':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_hour, 1, 12))) {
					return (0);
				}
				if (tm->tm_hour == 12) {
					tm->tm_hour = 0;
				}
				break;
			// The day of year.
			case 'j':
				LEGAL_ALT(0);
				if (!(conv_num(&bp, &i, 1, 366))) {
					return (0);
				}
				tm->tm_yday = i - 1;
				break;
			// The minute.
			case 'M':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_min, 0, 59))) {
					return (0);
				}
				break;
			// The month.     
			case 'm':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &i, 1, 12))) {
					return (0);
				}
				tm->tm_mon = i - 1;
				break;
//			// The locale's equivalent of AM/PM. 
//			case 'p':
//				LEGAL_ALT(0);
//				// AM?
//				if (strcasecmp(am_pm[0], bp) == 0) {
//					if (tm->tm_hour > 11) {
//						return (0);
//					}
//					bp += strlen(am_pm[0]);
//					break;
//				}
//				// PM?
//				else if (strcasecmp(am_pm[1], bp) == 0) {
//					if (tm->tm_hour > 11) {
//						return (0);
//					}
//					tm->tm_hour += 12;
//					bp += strlen(am_pm[1]);
//					break;
//				}
//
//				// Nothing matched.
//				return (0);
			// The seconds.     
			case 'S': 
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_sec, 0, 61))) {
					return (0);
				}
				break;
			// The week of year, beginning on sunday.
			case 'U':
			// The week of year, beginning on monday.
			case 'W':
				LEGAL_ALT(ALT_O);
				//XXX This is bogus, as we can not assume any valid
				//information present in the tm structure at this
				//point to calculate a real value, so just check the
				//range for now.
				if (!(conv_num(&bp, &i, 0, 53))) {
					return (0);
				}
				break;
			// The day of week, beginning on sunday. 
			case 'w':
				LEGAL_ALT(ALT_O);
				if (!(conv_num(&bp, &tm->tm_wday, 0, 6))) {
					return (0);
				}
				break;
			// The year. 
			case 'Y':
				LEGAL_ALT(ALT_E);
				if (!(conv_num(&bp, &i, 0, 9999))) {
					return (0);
				}
				tm->tm_year = i - TM_YEAR_BASE;
				break;
			// The year within 100 years of the epoch.     
			case 'y':
				LEGAL_ALT(ALT_E | ALT_O);
				if (!(conv_num(&bp, &i, 0, 99))) {
					return (0);
				}
				if (split_year) {
					tm->tm_year = ((tm->tm_year / 100) * 100) + i;
					break;
				}
				split_year = 1;
				if (i <= 68) {
					tm->tm_year = i + 2000 - TM_YEAR_BASE;
				}
				else {
					tm->tm_year = i + 1900 - TM_YEAR_BASE;
				}
				break;
 
			//Miscellaneous conversions.
			// Any kind of white-space.
			case 'n':
			case 't':
				LEGAL_ALT(0);
				while (isspace(*bp)) {
					bp++;
				}
				break;
			// Unknown/unsupported conversion. 
			default:
				return (0);
		}
  }
  // LINTED functional specification
  return ((char *)bp);
}
 
 
static int conv_num(const char **buf, int *dest, int llim, int ulim) {
	int result = 0;

	// The limit also determines the number of valid digits.
	int rulim = ulim;

	if (**buf < '0' || **buf > '9') {
		return (0);
	}
 
	do {
		result *= 10;
		result += *(*buf)++ - '0';
		rulim /= 10;
	}
	while ((result * 10 <= ulim) && rulim && **buf >= '0' && **buf <= '9');
 
	if (result < llim || result > ulim) {
		return (0);
	}
 
	*dest = result;
	return (1);
}
 
int strncasecmp(char *s1, char *s2, size_t n) {
	if (n == 0) {
		return 0;
	}
 
	while (n-- != 0 && tolower(*s1) == tolower(*s2)) {
		if (n == 0 || *s1 == '\0' || *s2 == '\0') {
			break;
		}
		s1++;
		s2++;
	}
 
	return tolower(*(unsigned char *) s1) - tolower(*(unsigned char *) s2);
}