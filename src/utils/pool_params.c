/* -*-pgsql-c-*- */
/*
 * $Header$
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2020	PgPool Global Development Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 *
 * params.c: Parameter Status handling routines
 *
 */
#include "config.h"

#include <stdlib.h>
#include <string.h>
#include "utils/elog.h"
#include "utils/pool_params.h"

#include "pool.h"
#include "parser/parser.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

/*
 * initialize parameter structure
 */
int
pool_init_params(ParamStatus * params)
{
	params->num = 0;
	return 0;
}

/*
 * discard parameter structure
 */
void
pool_discard_params(ParamStatus * params)
{
	params->num = 0;
}

/*
 * find param value by name. if found, its value is returned
 * also, pos is set
 * if not found, NULL is returned
 */
char *
pool_find_name(ParamStatus * params, char *name, int *pos)
{
	int			i;

	for (i = 0; i < params->num; i++)
	{
		if (!strcmp(name, params->params[i].name))
		{
			*pos = i;
			return params->params[i].value;
		}
	}
	return NULL;
}

/*
 * return name and value by index.
 */
int
pool_get_param(ParamStatus * params, int index, char **name, char **value)
{
	if (index < 0 || index >= params->num)
		return -1;

	*name = params->params[index].name;
	*value = params->params[index].value;

	return 0;
}

/*
 * add or replace name/value pair
 */
int
pool_add_param(ParamStatus * params, char *name, char *value)
{
	int			pos;
	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

	if (pool_find_name(params, name, &pos) == NULL)
	{
		if (params->num >= MAX_PARAM_ITEMS)
		{
			ereport(ERROR,
				(errmsg("add parameter failed"),
					errdetail("no more room for parameter number %d", params->num)));
		}
		pos = params->num;
		strncpy(params->params[pos].name, name,  sizeof(params->params[pos].name));
		params->num++;
	}
	strncpy(params->params[pos].value, value,  sizeof(params->params[pos].value));
	parser_set_param(name, value);
	MemoryContextSwitchTo(oldContext);

	return 0;
}

void
pool_param_debug_print(ParamStatus * params)
{
	int			i;

	for (i = 0; i < params->num; i++)
	{
		ereport(LOG,
				(errmsg("No.%d: name: %s value: %s", i, params->params[i].name, params->params[i].value)));
	}
}
