from typing import Literal

from app import app


class BaseApiRouteHandler:
  
  def __init__(self, base_url: str):
    self.base_url = base_url
    
  def __listen(self, method: Literal["GET", "POST", "DELETE"], api: str):
    def decorator(f):
      def vievFunc(**kwargs):
        return self.process(f, **kwargs)
      view_func = vievFunc
      view_func.__name__ = f"{method}__{api}__{f.__name__}"      
      app.add_url_rule(
          rule=f"{self.base_url}/{api}",
          endpoint=view_func.__name__,
          view_func=view_func,
          methods=[method]
      )
      return f
    return decorator
  
  def post(self, api: str):
    return self.__listen(method="POST", api=api)
  
  def get(self, api: str):
    return self.__listen(method="GET", api=api)
  
  def delete(self, api: str):
    return self.__listen(method="DELETE", api=api)

  def process(self, f, **kwargs):
    return f(**kwargs)
  