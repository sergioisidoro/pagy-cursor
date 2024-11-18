require 'pagy_cursor/pagy/cursor'
class Pagy

  module Backend ; private         # the whole module is private so no problem with including it in a controller

    # Return Pagy object and items
    def pagy_cursor(collection, vars={}, options={})
      pagy = Pagy::Cursor.new(pagy_cursor_get_vars(collection, vars))

      items =  pagy_cursor_get_items(collection, pagy, pagy.position)
      pagy.has_more =  pagy_cursor_has_more?(items, pagy)

      return pagy, items
    end

    def pagy_cursor_get_vars(collection, vars)
      vars[:arel_table] = collection.arel_table
      vars[:primary_key] = collection.primary_key
      vars[:backend] = 'sequence'
      vars
    end

    def pagy_cursor_get_items(collection, pagy, position=nil)
      if position.present?
        # First - query the last element - 1 query (no scan as primary key should be indexed)
        last = collection.where(pagy.arel_table[pagy.primary_key].eq(position)).first

        # Second - filter the collection according to the the pagy.order elements
        # if pagy.order is { :created_at => :desc , :id => :desc } we will need to make
        # a query like "created_at <= last.created_at AND NOT (created_at = last.created_at and id > last.id)"
        # So we will iterate through all the order parameters and incrementally add the sql comparison to exclude 
        # items that are filtered by before/after values of the last element.
        # The last element of the order will not have the equals part, as we don't have any other "tie" breakers
        
        order_pairs = pagy.order.dup.map { |key, value| [key, value] }
        sql_comparation = pagy.arel_table

        operations = {
          # order: { last: [predicate_operation, opposite_predicate_operation] }
          before: {
            desc: { last: [:lt, :gteq], not_last: [:lteq, :gt] },
            asc:  { last: [:gt, :lteq], not_last: [:gteq, :lt] }
          },
          after: {
            desc: { last: [:gt, :lteq], not_last: [:gteq, :lt] },
            asc:  { last: [:lt, :gteq], not_last: [:lteq, :gt] }
          }
        }

        previous_predicate = nil
        order_pairs.each_with_index do |predicate, index|
          # predicate is [attribute, :desc/:asc]
          # if it's the last element, we don't need to add the equals part

          is_last = index == order_pairs.length - 1

          if @after.present?
            predicate_operation = operations[:after][predicate[1]][is_last ? :last : :not_last][0]
            opposite_predicate_operation = operations[:after][predicate[1]][is_last ? :last : :not_last][1]
          else
            predicate_operation = operations[:before][predicate[1]][is_last ? :last : :not_last][0]
            opposite_predicate_operation = operations[:before][predicate[1]][is_last ? :last : :not_last][1]  
          end

          # If it's the first element, we need to add the sql_comparation without the 
          # tie breaker conditions
          if index == 0 || !previous_predicate
            sql_comparation = sql_comparation[predicate[0]].send(predicate_operation, last[predicate[0]])
            previous_predicate = predicate
          else
            # We need to add the previous predicates to the sql_comparation
            # Because of transitive property, we only need to check the last element
            # eg : { :created_at => :desc , :updated_at => :desc , :id => :desc }
            # We need to add the following condition:
            # created_at <= last.created_at 
            #   AND NOT (created_at = last.created_at and updated_at > last.updated_at) 
            #   AND NOT (updated_at = last.updated_at and id >= last.id)
            tie_condition = pagy.arel_table[previous_predicate[0]].eq(last[previous_predicate[0]]).and(
              pagy.arel_table[predicate[0]].send(opposite_predicate_operation, last[predicate[0]])
            ).not
            sql_comparation = sql_comparation.and(tie_condition)
          end
        end
        collection.where(sql_comparation).reorder(pagy.order).limit(pagy.items)
      else
        collection.reorder(pagy.order).limit(pagy.items)
      end
    end

    def pagy_cursor_has_more?(collection, pagy)
      return false if collection.empty?
      next_position = collection.last[pagy.primary_key]
      pagy_cursor_get_items(collection, pagy, next_position).exists?
    end
  end
end
