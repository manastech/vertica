#include <ruby.h>

static VALUE rb_mVertica, rb_cDate, rb_cDateTime;
static ID intern_new;
int year, month, day, tokens;

static short read_short(char** pstr) {
  unsigned char* str = (unsigned char*) *pstr;
  short h = (short) (*str++);
  short l = (short) (*str++);
  *pstr = (char*) str;
  return (h << 8) + l;
}

static unsigned int read_int(char** pstr) {
  unsigned char* str = (unsigned char*) *pstr;
  unsigned int a = (unsigned int) (*str++);
  unsigned int b = (unsigned int) (*str++);
  unsigned int c = (unsigned int) (*str++);
  unsigned int d = (unsigned int) (*str++);
  *pstr = (char*) str;
  return (a << 24) + (b << 16) + (c << 8) + d;
}

static VALUE rb_vertica_parse_date(VALUE self, VALUE string) {
  char* str = StringValuePtr(string);
  unsigned int year, month, day, tokens;
  tokens = sscanf(str, "%4d-%2d-%2d", &year, &month, &day);
  return rb_funcall(rb_cDate, intern_new, 3, INT2NUM(year), INT2NUM(month), INT2NUM(day));
}

static VALUE rb_vertica_parse_timestamp(VALUE self, VALUE string) {
  char* str = StringValuePtr(string);
  unsigned int year, month, day, hour, min, sec, tokens;
  tokens = sscanf(str, "%4d-%2d-%2d %2d:%2d:%2d", &year, &month, &day, &hour, &min, &sec);
  return rb_funcall(rb_cDateTime, intern_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
}

static VALUE rb_vertica_parse_timestamp_tz(VALUE self, VALUE string) {
  char* str = StringValuePtr(string);
  double sec;
  unsigned int year, month, day, hour, min, tokens, tz;
  tokens = sscanf(str, "%4d-%2d-%2d %2d:%2d:%lf%d", &year, &month, &day, &hour, &min, &sec, &tz);
  return rb_funcall(rb_cDateTime, intern_new, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), DBL2NUM(sec), INT2NUM(tz));
}

static VALUE rb_vertica_parse_data_row(VALUE self, VALUE string) {
  int i;
  unsigned int len;
  VALUE ary;
  VALUE value;
  char* str = StringValuePtr(string);
  short field_count = read_short(&str);
  ary = rb_ary_new2((int)field_count);
  for(i = 0; i < field_count; i++) {
    len = read_int(&str);
    if (len == 0xFFFFFFFF) {
      rb_ary_push(ary, Qnil);
    } else {
      value = rb_str_new(str, len);
      rb_ary_push(ary, value);
      str += len;
    }
  }
  return ary;
}

void Init_vertica(void) {
  rb_mVertica = rb_define_module("Vertica");
  rb_cDate = rb_const_get(rb_cObject, rb_intern("Date"));
  rb_cDateTime = rb_const_get(rb_cObject, rb_intern("DateTime"));

  intern_new = rb_intern("new");

  rb_define_singleton_method(rb_mVertica, "parse_date", rb_vertica_parse_date, 1);
  rb_define_singleton_method(rb_mVertica, "parse_timestamp", rb_vertica_parse_timestamp, 1);
  rb_define_singleton_method(rb_mVertica, "parse_timestamp_tz", rb_vertica_parse_timestamp_tz, 1);
  rb_define_singleton_method(rb_mVertica, "parse_data_row", rb_vertica_parse_data_row, 1);
}
